//go:build linux || darwin

package agent

import "syscall"

// syncDisks flushes written data to disk. Restores and swaps call it
// before they write a marker that says "the data is complete", so a power
// loss cannot leave the marker without the data.
func syncDisks() { syscall.Sync() }
