package agent

import (
	"os"
	"syscall"
)

func statExtra(info os.FileInfo) (ctimeNanos int64, inode uint64) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Ctim.Sec*1e9 + st.Ctim.Nsec, st.Ino
	}
	return 0, 0
}
