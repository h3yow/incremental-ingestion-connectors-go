package sink_test

import "os"

func openDir(dir string) (*os.File, error) { return os.Open(dir) }
