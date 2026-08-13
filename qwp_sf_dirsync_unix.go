//go:build !windows

package questdb

import "os"

func qwpSfSyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
