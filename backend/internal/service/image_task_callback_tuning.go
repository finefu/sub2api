package service

// YeaGrow execution plane: callback delivery throughput settings.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Only the claim batch, the delivery concurrency and the idle poll interval are
// tunable. The claim script, the 60 s lease, the retry backoff and the
// at-least-once semantics are unchanged.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	// ImageTaskCallbackBatchEnv: events claimed per round (1..256, default 64).
	ImageTaskCallbackBatchEnv = "IMAGE_TASK_CALLBACK_BATCH"
	// ImageTaskCallbackConcurrencyEnv: callbacks posted in parallel (1..128, default 32).
	ImageTaskCallbackConcurrencyEnv = "IMAGE_TASK_CALLBACK_CONCURRENCY"
	// ImageTaskCallbackPollIntervalEnv: idle wait between empty rounds, Go duration (50ms..10s, default 200ms).
	ImageTaskCallbackPollIntervalEnv = "IMAGE_TASK_CALLBACK_POLL_INTERVAL"

	DefaultImageTaskCallbackBatch        = 64
	DefaultImageTaskCallbackConcurrency  = 32
	DefaultImageTaskCallbackPollInterval = 200 * time.Millisecond

	minImageTaskCallbackBatch        = 1
	maxImageTaskCallbackBatch        = 256
	minImageTaskCallbackConcurrency  = 1
	maxImageTaskCallbackConcurrency  = 128
	minImageTaskCallbackPollInterval = 50 * time.Millisecond
	maxImageTaskCallbackPollInterval = 10 * time.Second

	// imageTaskCallbackMaxWaves bounds how long a claimed event may wait for a
	// delivery slot: waves × request timeout (3 × 15 s) must stay below the
	// 60 s lease, or another worker could re-claim an event that is still
	// queued locally and post it a second time concurrently.
	imageTaskCallbackMaxWaves = 3
)

// ImageTaskCallbackTuning holds the delivery throughput settings.
type ImageTaskCallbackTuning struct {
	Batch        int
	Concurrency  int
	PollInterval time.Duration
}

// DefaultImageTaskCallbackTuning returns the defaults.
func DefaultImageTaskCallbackTuning() ImageTaskCallbackTuning {
	return ImageTaskCallbackTuning{
		Batch:        DefaultImageTaskCallbackBatch,
		Concurrency:  DefaultImageTaskCallbackConcurrency,
		PollInterval: DefaultImageTaskCallbackPollInterval,
	}
}

// waves is the number of sequential delivery waves one claimed batch needs.
func (t ImageTaskCallbackTuning) waves() int {
	return (t.Batch + t.Concurrency - 1) / t.Concurrency
}

// ParseImageTaskCallbackTuning reads the three settings through lookup. Each
// unset value keeps its default; each invalid or out-of-range value falls back
// to its default with a warning. If batch and concurrency together would keep
// a claimed event waiting longer than the lease allows, both fall back.
func ParseImageTaskCallbackTuning(lookup func(string) string) (ImageTaskCallbackTuning, []string) {
	tuning := DefaultImageTaskCallbackTuning()
	var warnings []string
	parseInt := func(name string, min, max, def int) int {
		raw := strings.TrimSpace(lookup(name))
		if raw == "" {
			return def
		}
		v, err := strconv.Atoi(raw)
		if err != nil || v < min || v > max {
			warnings = append(warnings, fmt.Sprintf("%s=%q is not an integer in [%d,%d]; using default %d", name, raw, min, max, def))
			return def
		}
		return v
	}
	tuning.Batch = parseInt(ImageTaskCallbackBatchEnv, minImageTaskCallbackBatch, maxImageTaskCallbackBatch, DefaultImageTaskCallbackBatch)
	tuning.Concurrency = parseInt(ImageTaskCallbackConcurrencyEnv, minImageTaskCallbackConcurrency, maxImageTaskCallbackConcurrency, DefaultImageTaskCallbackConcurrency)
	if raw := strings.TrimSpace(lookup(ImageTaskCallbackPollIntervalEnv)); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v < minImageTaskCallbackPollInterval || v > maxImageTaskCallbackPollInterval {
			warnings = append(warnings, fmt.Sprintf("%s=%q is not a duration in [%s,%s]; using default %s", ImageTaskCallbackPollIntervalEnv, raw,
				minImageTaskCallbackPollInterval, maxImageTaskCallbackPollInterval, DefaultImageTaskCallbackPollInterval))
		} else {
			tuning.PollInterval = v
		}
	}
	if tuning.waves() > imageTaskCallbackMaxWaves {
		warnings = append(warnings, fmt.Sprintf("%s=%d with %s=%d needs %d delivery waves (max %d within the %s lease); using defaults %d/%d",
			ImageTaskCallbackBatchEnv, tuning.Batch, ImageTaskCallbackConcurrencyEnv, tuning.Concurrency, tuning.waves(),
			imageTaskCallbackMaxWaves, imageTaskCallbackLease, DefaultImageTaskCallbackBatch, DefaultImageTaskCallbackConcurrency))
		tuning.Batch, tuning.Concurrency = DefaultImageTaskCallbackBatch, DefaultImageTaskCallbackConcurrency
	}
	return tuning, warnings
}

// LoadImageTaskCallbackTuningFromEnv parses the settings from the environment and logs every fallback.
func LoadImageTaskCallbackTuningFromEnv() ImageTaskCallbackTuning {
	tuning, warnings := ParseImageTaskCallbackTuning(os.Getenv)
	for _, w := range warnings {
		logger.L().Warn("image_task.callback_tuning_invalid; using default", zap.String("detail", w))
	}
	return tuning
}

// normalized returns t if it is within bounds, otherwise the defaults.
func (t ImageTaskCallbackTuning) normalized() ImageTaskCallbackTuning {
	if t.Batch < minImageTaskCallbackBatch || t.Batch > maxImageTaskCallbackBatch ||
		t.Concurrency < minImageTaskCallbackConcurrency || t.Concurrency > maxImageTaskCallbackConcurrency ||
		t.PollInterval < minImageTaskCallbackPollInterval || t.PollInterval > maxImageTaskCallbackPollInterval ||
		t.waves() > imageTaskCallbackMaxWaves {
		return DefaultImageTaskCallbackTuning()
	}
	return t
}

// WithTuning replaces the throughput settings before Start; out-of-range
// settings are replaced by the defaults. It returns d for chaining.
func (d *ImageTaskCallbackDeliverer) WithTuning(t ImageTaskCallbackTuning) *ImageTaskCallbackDeliverer {
	d.tuning = t.normalized()
	return d
}

// Tuning reports the settings in use.
func (d *ImageTaskCallbackDeliverer) Tuning() ImageTaskCallbackTuning {
	return d.tuning
}
