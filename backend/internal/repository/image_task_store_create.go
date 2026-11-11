package repository

// YeaGrow execution plane (T2.1): atomic task creation for idempotent submits.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.ImageTaskStoreCreateIfAbsent = (*imageTaskStore)(nil)

// SaveIfAbsent writes the task only if no record with the same ID exists
// (Redis SET NX). It reports whether this call created the record.
func (s *imageTaskStore) SaveIfAbsent(ctx context.Context, task *service.ImageTaskRecord, ttl time.Duration) (bool, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return false, err
	}
	return s.rdb.SetNX(ctx, imageTaskKey(task.ID), data, ttl).Result()
}
