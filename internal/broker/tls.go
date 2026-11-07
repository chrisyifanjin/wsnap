package broker

import (
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// CertReloader serves a key pair from disk and reloads it when the files
// change, so certificates rotated by cert-manager take effect without a
// restart.
type CertReloader struct {
	CertFile, KeyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
	checked time.Time
}

// GetCertificate implements tls.Config.GetCertificate.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cert != nil && time.Since(r.checked) < 30*time.Second {
		return r.cert, nil
	}
	r.checked = time.Now()
	info, err := os.Stat(r.CertFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil
		}
		return nil, err
	}
	if r.cert != nil && info.ModTime().Equal(r.modTime) {
		return r.cert, nil
	}
	c, err := tls.LoadX509KeyPair(r.CertFile, r.KeyFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil // keep serving the old pair during a half-written rotation
		}
		return nil, err
	}
	r.cert, r.modTime = &c, info.ModTime()
	return r.cert, nil
}
