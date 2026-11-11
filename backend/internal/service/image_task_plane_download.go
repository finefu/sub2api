package service

// YeaGrow execution plane: bounded retry of the result-URL download.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Downloading the provider's result URL only reads an image that was already
// generated and billed; repeating the GET cannot generate or bill again, so
// it does not break the "no replay" rule. The upstream POST is never retried.
//
// The policy mirrors YeaGrow's direct path (lib/providers/result-get-retry.ts
// downloadResultWithRetry and its caller in kemeitv-field-assistant-image.ts):
//   - at most 2 extra attempts (3 in total), the same URL, one at a time;
//   - wait 1 s then 2 s (1 s * 2^attempt), or longer when Retry-After says so;
//   - a retry is only scheduled if it starts within 30 s of the first attempt;
//   - an attempt is aborted after 15 s without header or body progress;
//   - the whole delivery is bounded by min(90 s, result validity 15 min,
//     the task's remaining execution time);
//   - retryable: HTTP 429/500/502/503/504/524, transport errors (reset,
//     refused, DNS, timeouts), a body interrupted mid-read, the idle timeout.
//     Not retryable: other statuses (404, 403 expired signature, ...), the
//     SSRF guard refusing a destination, oversized or non-image content.
// Every attempt and every redirect hop goes through the same guarded client.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type imageTaskPlaneDownloadPolicy struct {
	MaxAttempts int           // total attempts, including the first
	BaseDelay   time.Duration // delay before retry n (1-based) = BaseDelay * 2^(n-1)
	RetryWindow time.Duration // a retry must start within this window of the first attempt
	Idle        time.Duration // abort an attempt after this long without progress
	Total       time.Duration // hard bound for the whole delivery
	URLValidity time.Duration // result URL validity assumed when the provider does not say
}

// imageTaskPlaneDownload is the production policy (YeaGrow parity). Tests may
// shorten it; production never changes it.
var imageTaskPlaneDownload = imageTaskPlaneDownloadPolicy{
	MaxAttempts: 3,
	BaseDelay:   time.Second,
	RetryWindow: 30 * time.Second,
	Idle:        15 * time.Second,
	Total:       90 * time.Second,
	URLValidity: 15 * time.Minute,
}

// imageTaskPlaneRetryableStatus mirrors YeaGrow's RETRYABLE set.
var imageTaskPlaneRetryableStatus = map[int]bool{429: true, 500: true, 502: true, 503: true, 504: true, 524: true}

var (
	errImageDownloadIdle      = errors.New("download image: no progress within the idle timeout")
	errImageDownloadTooLarge  = errors.New("download image: too large")
	errImageDownloadNotImage  = errors.New("download image: content is not an image")
	errImageDownloadNoBudget  = errors.New("download image: no time left before the delivery deadline")
	errImageDownloadExhausted = errors.New("download image: retry budget exhausted")
)

// imageDownloadStatusError is a non-2xx answer to the result GET.
type imageDownloadStatusError struct {
	Status     int
	RetryAfter string
}

func (e *imageDownloadStatusError) Error() string {
	return fmt.Sprintf("download image: unexpected status %d", e.Status)
}

// imageTaskPlaneDeliveryDeadline: min(start + Total, accepted + URLValidity,
// task created + execution timeout).
func imageTaskPlaneDeliveryDeadline(start, accepted time.Time, taskCreated int64, executionTimeout time.Duration) time.Time {
	p := imageTaskPlaneDownload
	deadline := start.Add(p.Total)
	if v := accepted.Add(p.URLValidity); v.Before(deadline) {
		deadline = v
	}
	if taskCreated > 0 && executionTimeout > 0 {
		if e := time.Unix(taskCreated, 0).Add(executionTimeout); e.Before(deadline) {
			deadline = e
		}
	}
	return deadline
}

// planeItemDownloadURL returns the item's remote URL when fetchImageBytes would
// download it (no inline b64_json, not a data: URL).
func planeItemDownloadURL(item map[string]json.RawMessage) (string, bool) {
	if raw, ok := item["b64_json"]; ok {
		var b64 string
		if json.Unmarshal(raw, &b64) == nil && strings.TrimSpace(b64) != "" {
			return "", false
		}
	}
	raw, ok := item["url"]
	if !ok {
		return "", false
	}
	var rawURL string
	if json.Unmarshal(raw, &rawURL) != nil {
		return "", false
	}
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" || (len(rawURL) >= len("data:") && strings.EqualFold(rawURL[:len("data:")], "data:")) {
		return "", false
	}
	return rawURL, true
}

// downloadPlaneResult downloads rawURL with the bounded retry policy. attempts
// is the number of GETs started (0 when the deadline had already passed).
func (u *ImageResultUploader) downloadPlaneResult(ctx context.Context, rawURL string, deadline time.Time) (data []byte, contentType string, attempts int, err error) {
	p := imageTaskPlaneDownload
	if !time.Now().Before(deadline) {
		return nil, "", 0, errImageDownloadNoBudget
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	windowEnd := time.Now().Add(p.RetryWindow)
	for attempts = 1; ; attempts++ {
		data, contentType, err = u.downloadPlaneAttempt(ctx, rawURL, p.Idle)
		if err == nil {
			return data, contentType, attempts, nil
		}
		if ctx.Err() != nil {
			return nil, "", attempts, fmt.Errorf("%w (delivery deadline reached)", err)
		}
		if !isRetryablePlaneDownloadError(err) {
			return nil, "", attempts, err
		}
		if attempts >= p.MaxAttempts {
			return nil, "", attempts, fmt.Errorf("%w after %d attempts: %w", errImageDownloadExhausted, attempts, err)
		}
		retryAfter := ""
		var statusErr *imageDownloadStatusError
		if errors.As(err, &statusErr) {
			retryAfter = statusErr.RetryAfter
		}
		delay := planeRetryDelay(retryAfter, attempts, p.BaseDelay, time.Now())
		if next := time.Now().Add(delay); !next.Before(windowEnd) || !next.Before(deadline) {
			return nil, "", attempts, fmt.Errorf("%w (retry window): %w", errImageDownloadExhausted, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, "", attempts, fmt.Errorf("%w (delivery deadline reached)", err)
		case <-timer.C:
		}
	}
}

// planeRetryDelay mirrors YeaGrow resultRetryDelay: base * 2^(n-1), or the
// Retry-After value (seconds or HTTP date) when it is longer.
func planeRetryDelay(retryAfter string, failedAttempt int, base time.Duration, now time.Time) time.Duration {
	fallback := base << (failedAttempt - 1)
	text := strings.TrimSpace(retryAfter)
	if text == "" {
		return fallback
	}
	if secs, err := strconv.ParseInt(text, 10, 64); err == nil && secs >= 0 {
		if secs > int64(time.Hour/time.Second) {
			return time.Hour // far beyond any window: the caller stops
		}
		return longerDuration(fallback, time.Duration(secs)*time.Second)
	}
	if when, err := http.ParseTime(text); err == nil {
		return longerDuration(fallback, when.Sub(now))
	}
	return fallback
}

func longerDuration(a, b time.Duration) time.Duration {
	if b > a {
		return b
	}
	return a
}

// downloadPlaneAttempt performs one guarded GET with an idle watchdog that is
// reset by the response headers and by every body read that makes progress.
func (u *ImageResultUploader) downloadPlaneAttempt(parent context.Context, rawURL string, idle time.Duration) ([]byte, string, error) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	watchdog := time.AfterFunc(idle, func() { cancel(errImageDownloadIdle) })
	defer watchdog.Stop()
	idleErr := func(err error) error {
		if errors.Is(context.Cause(ctx), errImageDownloadIdle) && parent.Err() == nil {
			return fmt.Errorf("%w: %w", errImageDownloadIdle, err)
		}
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build download request: %w", err)
	}
	req.Header.Set("Accept", "image/png, image/jpeg")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, "", idleErr(fmt.Errorf("download image: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	watchdog.Reset(idle)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, "", &imageDownloadStatusError{Status: resp.StatusCode, RetryAfter: resp.Header.Get("Retry-After")}
	}
	limit := u.maxDownloadBytes
	if limit <= 0 {
		limit = defaultImageMaxDownloadBytes
	}
	body := &progressReader{r: io.LimitReader(resp.Body, limit+1), progress: func() { watchdog.Reset(idle) }}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, "", idleErr(fmt.Errorf("read image body: %w", err))
	}
	if int64(len(data)) > limit {
		return nil, "", fmt.Errorf("%w: exceeds %d bytes", errImageDownloadTooLarge, limit)
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0]))
	if !strings.HasPrefix(contentType, "image/") {
		if contentType = detectedImageContentType(data); contentType == "" {
			return nil, "", errImageDownloadNotImage
		}
	}
	return data, contentType, nil
}

type progressReader struct {
	r        io.Reader
	progress func()
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.progress()
	}
	return n, err
}

// isRetryablePlaneDownloadError: transient failures only (see file comment).
func isRetryablePlaneDownloadError(err error) bool {
	if err == nil {
		return false
	}
	var status *imageDownloadStatusError
	if errors.As(err, &status) {
		return imageTaskPlaneRetryableStatus[status.Status]
	}
	if errors.Is(err, errImageDownloadIdle) {
		return true
	}
	if isImageDownloadDestinationBlocked(err) || errors.Is(err, errImageDownloadTooLarge) || errors.Is(err, errImageDownloadNotImage) {
		return false
	}
	// *url.Error itself implements net.Error; classify what it wraps.
	var urlErr *url.Error
	for errors.As(err, &urlErr) && urlErr.Err != nil {
		err = urlErr.Err
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) // dial/read errors, DNS failures, client timeouts
}

// isImageDownloadDestinationBlocked reports a refusal by the SSRF guard,
// either at the hostname layer or at the resolved-IP (socket) layer.
func isImageDownloadDestinationBlocked(err error) bool {
	if errors.Is(err, errImageDownloadHostNotAllowed) {
		return true
	}
	var addrErr *net.AddrError
	return errors.As(err, &addrErr) && addrErr.Err == imageDownloadAddressNotAllowed
}
