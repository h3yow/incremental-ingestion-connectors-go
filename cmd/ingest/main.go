// 命令 ingest 演示本地增量摄取：注册连接器、加载检查点、运行管线。
//
// 用法：
//
//	go run ./cmd/ingest -kind jsonl -path data.jsonl -out out.jsonl -ckpt ckpt.json
//	go run ./cmd/ingest -kind csv   -path data.csv   -out out.jsonl -ckpt ckpt.json
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/h3yow/incremental-ingestion-connectors-go/connectors/csv"
	"github.com/h3yow/incremental-ingestion-connectors-go/connectors/jsonl"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/checkpoint"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/connector"
	ierr "github.com/h3yow/incremental-ingestion-connectors-go/internal/errors"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/pipeline"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/reject"
	"github.com/h3yow/incremental-ingestion-connectors-go/internal/sink"
)

func main() {
	var (
		kind    = flag.String("kind", "jsonl", "connector kind (jsonl|csv)")
		path    = flag.String("path", "", "source data file path")
		name    = flag.String("name", "", "source instance name (default: <kind>:<path>)")
		out     = flag.String("out", "out/events.jsonl", "output file path")
		ckpt    = flag.String("ckpt", "out/checkpoint.json", "checkpoint file path")
		rejects = flag.String("rejects", "out/rejects.jsonl", "rejected records file path")
		workers = flag.Int("workers", 4, "number of concurrent workers")
		queue   = flag.Int("queue", 64, "queue capacity (backpressure boundary)")
		batch   = flag.Int("batch", 16, "batch size")
		verbose = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(*kind, *path, *name, *out, *ckpt, *rejects, *workers, *queue, *batch, log); err != nil {
		log.Error("ingest failed", "error_class", string(ierr.ClassOf(err)), "error", err)
		os.Exit(1)
	}
}

func run(kind, path, name, out, ckpt, rejectsPath string, workers, queue, batch int, log *slog.Logger) error {
	reg := connector.NewRegistry()
	for _, register := range []func(*connector.Registry) error{jsonl.Register, csv.Register} {
		if err := register(reg); err != nil {
			return err
		}
	}

	cfg := connector.Config{"path": path}
	if name != "" {
		cfg["name"] = name
	}
	src, err := reg.New(kind, cfg)
	if err != nil {
		return err
	}

	store := checkpoint.NewFileStore(ckpt)
	pos, err := store.Load(context.Background(), src.Name())
	if err != nil {
		return err
	}
	log.Info("resuming from checkpoint", "source", src.Name(), "offset", pos.Offset, "seq", pos.Seq)

	if err := src.Open(context.Background(), pos); err != nil {
		return err
	}
	defer src.Close()

	sk := sink.NewFileSink(out)
	defer sk.Close()

	rj := reject.NewFileStore(rejectsPath)
	defer rj.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p := pipeline.New(src, sk, store, pipeline.Options{
		Workers:     workers,
		QueueSize:   queue,
		BatchSize:   batch,
		RejectStore: rj,
		Logger:      log,
	})
	if err := p.Run(ctx); err != nil {
		return err
	}
	log.Info("ingest finished",
		"produced", p.Stats.Produced.Load(),
		"written", p.Stats.Written.Load(),
		"batches", p.Stats.Batches.Load(),
		"committed", p.Stats.Committed.Load(),
		"rejected", p.Stats.Rejected.Load())
	// 拒绝记录汇总：按类别统计，便于判断改配置还是改数据。
	if sum, err := rj.Summary(context.Background(), src.Name()); err == nil && len(sum) > 0 {
		log.Info("reject summary", "source", src.Name(), "by_class", sum)
	}
	fmt.Fprintf(os.Stderr, "output: %s\ncheckpoint: %s\nrejects: %s\n", out, ckpt, rejectsPath)
	return nil
}
