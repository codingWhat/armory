package distributedlock

import (
	"context"
	"time"
)

type Locker interface {
	Lock(ctx context.Context, key string, owner string, ttl time.Duration) error
	UnLock(ctx context.Context, key string, owner string) error
}
