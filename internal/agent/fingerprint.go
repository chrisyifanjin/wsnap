package agent

import (
	"encoding/binary"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/chrisyifanjin/wsnap/internal/engine"
)

// fingerprint summarizes a directory tree: every path with its size, mode,
// modification time, change time and inode. Equal fingerprints mean nothing
// changed, so the sidecar can skip a snapshot. It costs one lstat per entry,
// the same scan the engines do anyway, but no reads and no network.
//
// The change time (ctime) cannot be set from user space, so a write that
// restores the old mtime still changes the fingerprint. Renames and
// deletions change the parent directory's times and the path list.
type fingerprint uint64

func computeFingerprint(dir string) (fingerprint, error) {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v int64) {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if d.IsDir() && path != dir && (rel == engine.StateDirName || rel == "lost+found") {
			return filepath.SkipDir
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		h.Write([]byte(rel))
		h.Write([]byte{0})
		put(info.Size())
		put(int64(info.Mode()))
		put(info.ModTime().UnixNano())
		ctime, ino := statExtra(info)
		put(ctime)
		put(int64(ino))
		return nil
	})
	return fingerprint(h.Sum64()), err
}
