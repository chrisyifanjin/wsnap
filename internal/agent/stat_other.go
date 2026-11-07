//go:build !linux && !darwin

package agent

import "os"

// statExtra has no portable source of ctime and inode; mtime, size and mode
// still detect ordinary writes.
func statExtra(os.FileInfo) (int64, uint64) { return 0, 0 }
