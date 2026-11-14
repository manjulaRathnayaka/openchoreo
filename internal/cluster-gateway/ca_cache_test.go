// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package clustergateway

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func caSecret(ns, name, data string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string][]byte{"ca.crt": []byte(data)},
	}
}

// countingClient returns a fake client that counts Secret reads.
func countingClient(t *testing.T, gets *atomic.Int64, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					gets.Add(1)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
}

func TestSecretCACache_ServesFromCache(t *testing.T) {
	var gets atomic.Int64
	c := newSecretCACache(countingClient(t, &gets, caSecret("ns", "ca", "A")), time.Hour, testLogger())
	key := secretCAKey{Namespace: "ns", Name: "ca", Key: "ca.crt"}

	for range 5 {
		data, err := c.get(context.Background(), key, false)
		require.NoError(t, err)
		assert.Equal(t, "A", string(data))
	}
	assert.Equal(t, int64(1), gets.Load(), "only the first get should read the Secret")
}

func TestSecretCACache_ForceRereads(t *testing.T) {
	var gets atomic.Int64
	cl := countingClient(t, &gets, caSecret("ns", "ca", "A"))
	c := newSecretCACache(cl, time.Hour, testLogger())
	key := secretCAKey{Namespace: "ns", Name: "ca", Key: "ca.crt"}

	_, err := c.get(context.Background(), key, false)
	require.NoError(t, err)

	require.NoError(t, cl.Update(context.Background(), caSecret("ns", "ca", "B")))
	data, err := c.get(context.Background(), key, true)
	require.NoError(t, err)
	assert.Equal(t, "B", string(data), "force must return the current Secret content")

	data, err = c.get(context.Background(), key, false)
	require.NoError(t, err)
	assert.Equal(t, "B", string(data), "forced read must update the cache")
}

func TestSecretCACache_StaleServedThenRefreshed(t *testing.T) {
	var gets atomic.Int64
	cl := countingClient(t, &gets, caSecret("ns", "ca", "A"))
	c := newSecretCACache(cl, 10*time.Millisecond, testLogger())
	key := secretCAKey{Namespace: "ns", Name: "ca", Key: "ca.crt"}

	_, err := c.get(context.Background(), key, false)
	require.NoError(t, err)
	require.NoError(t, cl.Update(context.Background(), caSecret("ns", "ca", "B")))
	time.Sleep(20 * time.Millisecond)

	// Expired: the stale value is returned at once, refresh runs in background.
	data, err := c.get(context.Background(), key, false)
	require.NoError(t, err)
	assert.Equal(t, "A", string(data))

	assert.Eventually(t, func() bool {
		d, _ := c.get(context.Background(), key, false)
		return string(d) == "B"
	}, time.Second, 5*time.Millisecond)
}

func TestSecretCACache_ErrorsNotCached(t *testing.T) {
	var gets atomic.Int64
	cl := countingClient(t, &gets)
	c := newSecretCACache(cl, time.Hour, testLogger())
	key := secretCAKey{Namespace: "ns", Name: "ca", Key: "ca.crt"}

	_, err := c.get(context.Background(), key, false)
	require.Error(t, err)

	require.NoError(t, cl.Create(context.Background(), caSecret("ns", "ca", "A")))
	data, err := c.get(context.Background(), key, false)
	require.NoError(t, err, "a missing Secret must be re-read once it exists")
	assert.Equal(t, "A", string(data))
}

func TestSecretCACache_EnsureLoadsMissingOnce(t *testing.T) {
	var gets atomic.Int64
	objs := make([]client.Object, 0, 50)
	keys := make([]secretCAKey, 0, 100)
	for i := range 50 {
		ns := fmt.Sprintf("org-%d", i)
		objs = append(objs, caSecret(ns, "ca", ns))
		k := secretCAKey{Namespace: ns, Name: "ca", Key: "ca.crt"}
		keys = append(keys, k, k) // duplicates must be read once
	}
	c := newSecretCACache(countingClient(t, &gets, objs...), time.Hour, testLogger())

	c.ensure(context.Background(), keys)
	assert.Equal(t, int64(50), gets.Load())
	assert.Equal(t, 50, c.len())

	c.ensure(context.Background(), keys)
	assert.Equal(t, int64(50), gets.Load(), "a warm cache must not read again")
}
