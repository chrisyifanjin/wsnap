package broker

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// PodLimiter is a token bucket per Pod UID. It stops one namespace from
// exhausting the broker or the cloud STS quota that all tenants share.
type PodLimiter struct {
	Rate  rate.Limit
	Burst int

	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

type bucket struct {
	l    *rate.Limiter
	seen time.Time
}

// Allow reports whether the Pod may make another request now.
func (p *PodLimiter) Allow(podUID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if p.buckets == nil {
		p.buckets = map[string]*bucket{}
	}
	// Drop idle buckets so deleted Pods do not leak memory.
	if now.Sub(p.swept) > 10*time.Minute {
		for k, b := range p.buckets {
			if now.Sub(b.seen) > 10*time.Minute {
				delete(p.buckets, k)
			}
		}
		p.swept = now
	}
	b, ok := p.buckets[podUID]
	if !ok {
		b = &bucket{l: rate.NewLimiter(p.Rate, p.Burst)}
		p.buckets[podUID] = b
	}
	b.seen = now
	return b.l.Allow()
}

// CachingAuthenticator remembers successful token reviews for a short
// time. Each sidecar request would otherwise cost one TokenReview call to
// the API server. The broker still reads the Pod live and checks its UID,
// so a deleted Pod's token stops working immediately.
type CachingAuthenticator struct {
	Next Authenticator
	TTL  time.Duration

	mu    sync.Mutex
	cache map[[32]byte]cachedIdentity
}

type cachedIdentity struct {
	id      Identity
	err     error
	expires time.Time
}

// failureTTL is how long a rejected token stays rejected without another
// TokenReview.
const failureTTL = 10 * time.Second

func (c *CachingAuthenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	key := sha256.Sum256([]byte(token))
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.cache[key]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.id, e.err
	}
	c.mu.Unlock()
	id, err := c.Next.Authenticate(ctx, token)
	if err != nil {
		c.mu.Lock()
		if c.cache == nil {
			c.cache = map[[32]byte]cachedIdentity{}
		}
		c.cache[key] = cachedIdentity{err: err, expires: now.Add(failureTTL)}
		c.mu.Unlock()
		return id, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cache == nil {
		c.cache = map[[32]byte]cachedIdentity{}
	}
	for k, e := range c.cache {
		if now.After(e.expires) {
			delete(c.cache, k)
		}
	}
	c.cache[key] = cachedIdentity{id: id, expires: now.Add(c.TTL)}
	return id, nil
}
