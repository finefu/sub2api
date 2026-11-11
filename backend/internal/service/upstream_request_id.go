package service

import (
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/tidwall/gjson"
	"golang.org/x/net/http/httpguts"
)

// AccountExtraUpstreamRequestIDHeader 是账户 extra 中的键，值为直接上游声明请求标识的响应头名。
// 未指定时不记录上游请求标识。
const AccountExtraUpstreamRequestIDHeader = "upstream_request_id_header"

const (
	maxUpstreamRequestIDHeaderNameLen = 64
	// maxUsageUpstreamRequestIDLen 与 usage_logs.upstream_request_id VARCHAR(128) 对齐。
	maxUsageUpstreamRequestIDLen = 128
)

// UpstreamRequestIDHeaderName 返回账户指定的上游请求标识头名，未指定时为空串。
func UpstreamRequestIDHeaderName(account *Account) string {
	if account == nil {
		return ""
	}
	return strings.TrimSpace(account.GetExtraString(AccountExtraUpstreamRequestIDHeader))
}

// UpstreamRequestIDFromHeaders 从直接上游的响应头解析请求标识。
// 只读账户指定的头；账户未指定头名时恒为空串。
func UpstreamRequestIDFromHeaders(account *Account, h http.Header) string {
	if len(h) == 0 {
		return ""
	}
	name := UpstreamRequestIDHeaderName(account)
	if name == "" {
		return ""
	}
	return strings.TrimSpace(h.Get(name))
}

// usageUpstreamRequestIDPtr 生成落库到 usage_logs.upstream_request_id 的值。
// WS 轮次没有 HTTP 响应头，保持 nil；超长时截断到列宽而不是让整条用量行失败。
func usageUpstreamRequestIDPtr(account *Account, h http.Header, wsMode bool) *string {
	if wsMode {
		return nil
	}
	id := UpstreamRequestIDFromHeaders(account, h)
	if id == "" {
		return nil
	}
	if len(id) > maxUsageUpstreamRequestIDLen {
		id = id[:maxUsageUpstreamRequestIDLen]
		for len(id) > 0 && !utf8.ValidString(id) {
			id = id[:len(id)-1]
		}
	}
	if id == "" {
		return nil
	}
	return &id
}

// ValidateUpstreamRequestIDHeaderExtra 校验并规范化 extra 中的上游请求标识头名：
// 必须是合法的 HTTP 头字段名且不超过 64 字节；空白值视为未指定并从 extra 中移除。
func ValidateUpstreamRequestIDHeaderExtra(extra map[string]any) error {
	if extra == nil {
		return nil
	}
	raw, ok := extra[AccountExtraUpstreamRequestIDHeader]
	if !ok || raw == nil {
		return nil
	}
	name, ok := raw.(string)
	if !ok {
		return infraerrors.BadRequest("INVALID_UPSTREAM_REQUEST_ID_HEADER",
			"upstream_request_id_header must be a string")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		delete(extra, AccountExtraUpstreamRequestIDHeader)
		return nil
	}
	if len(name) > maxUpstreamRequestIDHeaderNameLen || !httpguts.ValidHeaderFieldName(name) {
		return infraerrors.BadRequest("INVALID_UPSTREAM_REQUEST_ID_HEADER",
			"upstream_request_id_header must be a valid HTTP header name of at most 64 bytes")
	}
	extra[AccountExtraUpstreamRequestIDHeader] = name
	return nil
}

// ---- YeaGrow T2.6: client request ID passthrough and response-body fallback ----

// ClientRequestIDHeader carries the caller's correlation ID (YeaGrow's
// dispatchId) to the upstream on the Images path only.
const ClientRequestIDHeader = "X-Client-Request-Id"

// safeCorrelationID matches IDs that are safe to forward and to store:
// ASCII letters, digits and ._:- (YeaGrow SAFE_PROVIDER_REQUEST_ID), at most
// the usage_logs column width.
var safeCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// SanitizeCorrelationID returns id trimmed if it is a safe correlation ID, else "".
func SanitizeCorrelationID(id string) string {
	id = strings.TrimSpace(id)
	if !safeCorrelationID.MatchString(id) {
		return ""
	}
	return id
}

// ClientRequestIDForUpstream returns the caller-supplied X-Client-Request-Id
// when it is a safe ID. The gateway's own generated correlation ID is never
// forwarded.
func ClientRequestIDForUpstream(h http.Header) string {
	if h == nil {
		return ""
	}
	return SanitizeCorrelationID(h.Get(ClientRequestIDHeader))
}

// setClientRequestIDHeader sets the header after account header overrides so
// an override cannot replace the caller's ID; any casing variant is removed first.
func setClientRequestIDHeader(h http.Header, id string) {
	if h == nil || id == "" {
		return
	}
	for existing := range h {
		if strings.EqualFold(existing, ClientRequestIDHeader) {
			delete(h, existing)
		}
	}
	h.Set(ClientRequestIDHeader, id)
}

// UpstreamRequestIDFromJSONBody returns the top-level "id" of an upstream JSON
// response (OpenAI-compatible image APIs such as Kemei return the task ID
// there and no request-ID header), or "" if absent or unsafe.
func UpstreamRequestIDFromJSONBody(body []byte) string {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	id := gjson.GetBytes(body, "id")
	if id.Type != gjson.String {
		return ""
	}
	return SanitizeCorrelationID(id.String())
}

// usageUpstreamRequestIDPtrWithBodyFallback is usageUpstreamRequestIDPtr with
// the response-body ID as fallback when the configured header is missing.
func usageUpstreamRequestIDPtrWithBodyFallback(account *Account, h http.Header, wsMode bool, bodyID string) *string {
	if id := usageUpstreamRequestIDPtr(account, h, wsMode); id != nil || wsMode {
		return id
	}
	if bodyID = SanitizeCorrelationID(bodyID); bodyID != "" {
		return &bodyID
	}
	return nil
}

// AccountResponsesProbeEnabledEnv toggles the Responses-API capability probe
// that runs when an OpenAI API-key account is created, re-keyed or imported
// (YeaGrow T2.6). Default: off, so adding an image-only account sends nothing
// to the upstream.
const AccountResponsesProbeEnabledEnv = "ACCOUNT_RESPONSES_PROBE_ENABLED"

// AccountResponsesProbeEnabled reports whether the probe is enabled; an
// unparsable value counts as off.
func AccountResponsesProbeEnabled() bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(AccountResponsesProbeEnabledEnv)))
	return err == nil && enabled
}
