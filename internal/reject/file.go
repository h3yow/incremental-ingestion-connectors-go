package reject

import (
	"bufio"
	"encoding/json"
	stderrors "errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
)

func strconvFormatInt(v int64) string { return strconv.FormatInt(v, 10) }

// loadLocked 惰性加载：打开时从文件重建去重集合与内存索引（调用方已持有锁）。
func (s *FileStore) loadLocked() error {
	const op = "reject.FileStore.load"
	if s.seen != nil {
		return nil
	}
	s.seen = map[string]struct{}{}
	f, err := os.Open(s.path)
	switch {
	case err == nil:
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		for sc.Scan() {
			var r Rejection
			if json.Unmarshal(sc.Bytes(), &r) == nil {
				k := key(r.Source, r.Offset)
				if _, dup := s.seen[k]; !dup {
					s.seen[k] = struct{}{}
					s.recs = append(s.recs, r)
				}
			}
		}
		if err := sc.Err(); err != nil {
			return ierr.Wrap(ierr.ClassInternal, op, err)
		}
	case stderrors.Is(err, fs.ErrNotExist):
	default:
		return ierr.Wrap(ierr.ClassInternal, op, err)
	}
	return nil
}

// addLocked 去重后追加写并 fsync（调用方已持有锁）。
func (s *FileStore) addLocked(r Rejection) (bool, error) {
	const op = "reject.FileStore.add"
	if err := s.loadLocked(); err != nil {
		return false, err
	}
	k := key(r.Source, r.Offset)
	if _, dup := s.seen[k]; dup {
		return false, nil // 已登记过：重启重扫旧区间不重复登记
	}
	data, err := json.Marshal(r)
	if err != nil {
		return false, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return false, ierr.Wrap(ierr.ClassInternal, op, err)
		}
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return false, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return false, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	if err := f.Sync(); err != nil {
		return false, ierr.Wrap(ierr.ClassInternal, op, err)
	}
	s.seen[k] = struct{}{}
	s.recs = append(s.recs, r)
	return true, nil
}
