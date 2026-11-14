// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package clustergateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// defaultCASecretTTL is how long a CA bundle read from a Secret is served
	// before it is refreshed in the background. A stale entry is still served
	// while the refresh runs, so expiry never puts a Secret read back on the
	// agent connect path.
	defaultCASecretTTL = 5 * time.Minute
	// caFetchWorkers bounds concurrent Secret reads when many entries are
	// missing at once (gateway start, or a connect after many new CRs).
	caFetchWorkers = 16
	// caFetchTimeout bounds one Secret read.
	caFetchTimeout = 5 * time.Second
)

// secretCAKey identifies one CA bundle: a key inside a Secret.
type secretCAKey struct {
	Namespace string
	Name      string
	Key       string
}

func (k secretCAKey) String() string { return fmt.Sprintf("%s/%s[%s]", k.Namespace, k.Name, k.Key) }

type secretCAEntry struct {
	data       []byte
	fetchedAt  time.Time
	refreshing bool
}

// secretCACache caches the client CA bundles that plane CRs reference by
// SecretKeyRef.
//
// Without it every agent connect reads one Secret per plane CR sharing the
// agent's planeID. With hundreds of tenant CRs on one physical plane that is
// hundreds of API calls per connect, which outlasts the agent's handshake
// timeout so the agent can never connect. Only Secrets that plane CRs
// reference are read, by name; the gateway does not watch Secrets.
type secretCACache struct {
	reader client.Reader
	ttl    time.Duration
	logger *slog.Logger

	mu      sync.Mutex
	entries map[secretCAKey]*secretCAEntry

	misses atomic.Int64 // synchronous reads on the request path (for observability)

	// refreshSem bounds background refreshes: when many entries expire
	// together (they were warmed together) a connect must not start one
	// goroutine and one API call per CR at once.
	refreshSem chan struct{}
}

func newSecretCACache(reader client.Reader, ttl time.Duration, logger *slog.Logger) *secretCACache {
	if ttl <= 0 {
		ttl = defaultCASecretTTL
	}
	return &secretCACache{
		reader:     reader,
		ttl:        ttl,
		logger:     logger,
		entries:    make(map[secretCAKey]*secretCAEntry),
		refreshSem: make(chan struct{}, caFetchWorkers),
	}
}

// get returns the CA bundle for key. A cached entry is returned immediately,
// and refreshed in the background once older than the TTL. force bypasses the
// cache and reads the Secret now (used when a plane CR is created or updated).
func (c *secretCACache) get(ctx context.Context, key secretCAKey, force bool) ([]byte, error) {
	if !force {
		c.mu.Lock()
		e, ok := c.entries[key]
		if ok {
			data := e.data
			if time.Since(e.fetchedAt) > c.ttl && !e.refreshing {
				e.refreshing = true
				go c.refresh(key)
			}
			c.mu.Unlock()
			return data, nil
		}
		c.mu.Unlock()
	}
	c.misses.Add(1)
	return c.fetch(ctx, key)
}

// ensure loads every missing key with bounded concurrency, so a cold cache
// costs one parallel round of reads rather than a serial one.
func (c *secretCACache) ensure(ctx context.Context, keys []secretCAKey) {
	c.mu.Lock()
	missing := make([]secretCAKey, 0)
	seen := make(map[secretCAKey]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if _, ok := c.entries[k]; !ok {
			missing = append(missing, k)
		}
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return
	}

	c.misses.Add(int64(len(missing)))
	sem := make(chan struct{}, caFetchWorkers)
	var wg sync.WaitGroup
	for _, k := range missing {
		wg.Add(1)
		sem <- struct{}{}
		go func(k secretCAKey) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := c.fetch(ctx, k); err != nil {
				c.logger.Warn("failed to load CA secret", "secret", k.String(), "error", err)
			}
		}(k)
	}
	wg.Wait()
}

func (c *secretCACache) refresh(key secretCAKey) {
	c.refreshSem <- struct{}{}
	defer func() { <-c.refreshSem }()
	ctx, cancel := context.WithTimeout(context.Background(), caFetchTimeout)
	defer cancel()
	if _, err := c.fetch(ctx, key); err != nil {
		// Keep serving the last good bundle; retry on the next access.
		c.mu.Lock()
		if e, ok := c.entries[key]; ok {
			e.refreshing = false
		}
		c.mu.Unlock()
		c.logger.Warn("background CA secret refresh failed; serving last known bundle",
			"secret", key.String(), "error", err)
	}
}

// fetch reads the Secret and stores the result. Errors are not cached, and a
// failed read leaves any previous entry in place.
func (c *secretCACache) fetch(ctx context.Context, key secretCAKey) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, caFetchTimeout)
	defer cancel()

	var secret corev1.Secret
	if err := c.reader.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, &secret); err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", key.Namespace, key.Name, err)
	}
	data, ok := secret.Data[key.Key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in secret %s/%s", key.Key, key.Namespace, key.Name)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("CA data is empty in secret %s/%s key %s", key.Namespace, key.Name, key.Key)
	}

	c.mu.Lock()
	c.entries[key] = &secretCAEntry{data: data, fetchedAt: time.Now()}
	c.mu.Unlock()

	c.logger.Info("loaded CA certificate from secret",
		"secretName", key.Name,
		"namespace", key.Namespace,
		"key", key.Key,
		"dataSize", len(data),
	)
	return data, nil
}

// len reports the number of cached bundles.
func (c *secretCACache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
