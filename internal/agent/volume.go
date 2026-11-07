package agent

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrisyifanjin/wsnap/internal/engine"
)

// volume performs file operations on the workspace volume through an
// os.Root, which resolves every path, symlinks included, inside the
// volume. The workload can write anywhere in the volume, including
// .wsnap, so it can plant symlinks; through os.Root an absolute or "../"
// symlink fails instead of leading the agent into its own filesystem.
//
// Methods take absolute paths under the volume, as the rest of the agent
// builds them, and refuse anything outside it.
type volume struct {
	root *os.Root
	base string
}

// openVolume opens dir and checks that .wsnap, if present, is a real
// directory owned by the agent.
func openVolume(dir string) (*volume, error) {
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	v := &volume{root: r, base: filepath.Clean(dir)}
	if err := v.checkStateDir(); err != nil {
		r.Close()
		return nil, err
	}
	return v, nil
}

func (v *volume) Close() error { return v.root.Close() }

// checkStateDir refuses a .wsnap the workload replaced: a symlink, a file,
// or a directory owned by another user.
func (v *volume) checkStateDir() error {
	info, err := v.root.Lstat(engine.StateDirName)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("refusing to use %s: it is not a plain directory (mode %s); the workload may have replaced it", engine.StateDirName, info.Mode())
	}
	uid, ok := ownerUID(info)
	if !ok || uid == os.Geteuid() {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("refusing to use %s: owned by uid %d, not by the agent (uid %d); the workload may have replaced it", engine.StateDirName, uid, os.Geteuid())
	}
	// A root agent takes the directory over instead of failing: an agent
	// that ran under another uid (an older version, or a Pod-level
	// runAsUser) created it, and a refusal would block every later sidecar
	// and mover on this volume. Lchown never follows a symlink, and the
	// check below runs again on the result. After this, a non-root
	// workload can no longer rename entries inside it.
	if err := v.root.Lchown(engine.StateDirName, 0, 0); err != nil {
		return fmt.Errorf("take over %s: %w", engine.StateDirName, err)
	}
	if err := v.root.Chmod(engine.StateDirName, 0o755); err != nil {
		return fmt.Errorf("take over %s: %w", engine.StateDirName, err)
	}
	info, err = v.root.Lstat(engine.StateDirName)
	if err != nil {
		return err
	}
	if uid, ok := ownerUID(info); info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() || (ok && uid != 0) {
		return fmt.Errorf("refusing to use %s: it changed while the agent took it over", engine.StateDirName)
	}
	return nil
}

func (v *volume) rel(p string) (string, error) {
	r, err := filepath.Rel(v.base, filepath.Clean(p))
	if err != nil || r == ".." || strings.HasPrefix(r, "../") {
		return "", fmt.Errorf("path %s is outside the volume %s", p, v.base)
	}
	return r, nil
}

func (v *volume) Lstat(p string) (fs.FileInfo, error) {
	r, err := v.rel(p)
	if err != nil {
		return nil, err
	}
	return v.root.Lstat(r)
}

func (v *volume) Stat(p string) (fs.FileInfo, error) {
	r, err := v.rel(p)
	if err != nil {
		return nil, err
	}
	return v.root.Stat(r)
}

func (v *volume) ReadFile(p string) ([]byte, error) {
	r, err := v.rel(p)
	if err != nil {
		return nil, err
	}
	return v.root.ReadFile(r)
}

func (v *volume) WriteFile(p string, data []byte, perm os.FileMode) error {
	r, err := v.rel(p)
	if err != nil {
		return err
	}
	return v.root.WriteFile(r, data, perm)
}

func (v *volume) Remove(p string) error {
	r, err := v.rel(p)
	if err != nil {
		return err
	}
	return v.root.Remove(r)
}

func (v *volume) RemoveAll(p string) error {
	r, err := v.rel(p)
	if err != nil {
		return err
	}
	return v.root.RemoveAll(r)
}

func (v *volume) Rename(oldp, newp string) error {
	o, err := v.rel(oldp)
	if err != nil {
		return err
	}
	n, err := v.rel(newp)
	if err != nil {
		return err
	}
	return v.root.Rename(o, n)
}

func (v *volume) MkdirAll(p string, perm os.FileMode) error {
	r, err := v.rel(p)
	if err != nil {
		return err
	}
	return v.root.MkdirAll(r, perm)
}

func (v *volume) Chmod(p string, mode os.FileMode) error {
	r, err := v.rel(p)
	if err != nil {
		return err
	}
	return v.root.Chmod(r, mode)
}

func (v *volume) ReadDir(p string) ([]os.DirEntry, error) {
	r, err := v.rel(p)
	if err != nil {
		return nil, err
	}
	f, err := v.root.Open(r)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.ReadDir(-1)
}

// plainDir checks that p is a directory and not a symlink, right before
// its path is handed to an engine that resolves paths itself.
func (v *volume) plainDir(p string) error {
	info, err := v.Lstat(p)
	if err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("refusing to use %s: not a plain directory", p)
	}
	return nil
}
