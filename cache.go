package cache

import (
	"context"
	"errors"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
)

type Backend struct {
	prefix string
	client redis.UniversalClient
}

func NewBackend(prefix string, client redis.UniversalClient) Backend {
	return Backend{prefix: prefix, client: client}
}

func (b Backend) Get(ctx context.Context, key string) ([]byte, error) {
	return b.client.Get(ctx, b.prefix+key).Bytes()
}

func (b Backend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return b.client.Set(ctx, b.prefix+key, unsafe.String(unsafe.SliceData(value), len(value)), ttl).Err()
}

func (b Backend) Del(ctx context.Context, key string) error {
	return b.client.Del(ctx, b.prefix+key).Err()
}

func (b Backend) DelAll(ctx context.Context) (err error) {
	iter := b.client.Scan(ctx, 0, b.prefix+"*", 0).Iterator()

	for iter.Next(ctx) {
		err = errors.Join(err, b.client.Del(ctx, iter.Val()).Err())
	}

	return errors.Join(err, iter.Err())
}
