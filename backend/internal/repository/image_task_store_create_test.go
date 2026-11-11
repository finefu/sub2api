package repository

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestImageTaskStoreSaveIfAbsent(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewImageTaskStore(rdb)
	creator, ok := store.(service.ImageTaskStoreCreateIfAbsent)
	require.True(t, ok)
	ctx := context.Background()

	task := &service.ImageTaskRecord{ID: "imgtask_abc", UserID: 7, APIKeyID: 9, Status: service.ImageTaskStatusProcessing}
	created, err := creator.SaveIfAbsent(ctx, task, time.Hour)
	require.NoError(t, err)
	require.True(t, created)
	require.Greater(t, mr.TTL("image_task:imgtask_abc"), time.Duration(0))

	again := *task
	again.Status = service.ImageTaskStatusCompleted
	created, err = creator.SaveIfAbsent(ctx, &again, time.Hour)
	require.NoError(t, err)
	require.False(t, created)
	got, err := store.Get(ctx, "imgtask_abc")
	require.NoError(t, err)
	require.Equal(t, service.ImageTaskStatusProcessing, got.Status, "existing record is not overwritten")

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := creator.SaveIfAbsent(ctx, &service.ImageTaskRecord{ID: "imgtask_race", UserID: 7, APIKeyID: 9}, time.Hour)
			require.NoError(t, err)
			if ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), wins.Load())
}
