//go:build !linux && !darwin

package agent

import "io/fs"

func ownerUID(fs.FileInfo) (int, bool) { return 0, false }
