//go:build linux || darwin

package agent

import (
	"io/fs"
	"syscall"
)

func ownerUID(info fs.FileInfo) (int, bool) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid), true
	}
	return 0, false
}
