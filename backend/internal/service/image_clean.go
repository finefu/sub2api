package service

// YeaGrow execution plane (T2.5): generated-image metadata cleaning, ported
// byte-for-byte from YeaGrow lib/generated-image-metadata.ts (PNG),
// lib/generated-image-jpeg-metadata.ts (JPEG) and the dimension rule in
// lib/generated-image-validation.ts + lib/banana-image-dimensions.ts.
// Source: finefu/yeagrow 215bd837d49297d7206246f60d37b8e5fdefe5e8.
// Origin of this file: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).
//
// Verified against golden samples exported from YeaGrow's own functions
// (testdata/imageclean): identical output bytes for accepted inputs and the
// same error code for rejected ones. The cleaners never decode or re-encode
// pixels; they remove or rewrite metadata containers and validate the rest
// (PNG: chunks, zlib/DEFLATE, scanlines; JPEG: segments, Huffman entropy data).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"runtime"
)

const (
	imageCleanMaxBytes     = 10 * 1024 * 1024
	imageCleanMaxDimension = 4096
	imageCleanMaxPixels    = 16_777_216
)

// ImageCleanError reports why YeaGrow's cleaning rules reject an image. Code
// is the YeaGrow error code (e.g. "invalid_png_data", "unsupported_jpeg_app").
type ImageCleanError struct {
	Code string
}

func (e *ImageCleanError) Error() string { return "image clean rejected: " + e.Code }

// imageCleanAbort carries a YeaGrow fail(code) through the recursive parsers.
type imageCleanAbort struct{ code string }

func cleanFail(code string) {
	panic(imageCleanAbort{code: code})
}

// runImageClean converts aborts to *ImageCleanError. An out-of-range access
// (YeaGrow would throw a RangeError) is reported as "range_error".
func runImageClean(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case imageCleanAbort:
				err = &ImageCleanError{Code: v.code}
			case runtime.Error:
				err = &ImageCleanError{Code: "range_error"}
			default:
				panic(r)
			}
		}
	}()
	fn()
	return nil
}

// ImageCleanResult is the cleaned image and what YeaGrow reports about it.
type ImageCleanResult struct {
	Bytes        []byte
	MIMEType     string
	Width        int
	Height       int
	SHA256       string
	SourceSHA256 string
	Changed      bool
}

// CleanGeneratedImage applies YeaGrow's cleaner for the given MIME type
// ("image/png" or "image/jpeg"). Anything YeaGrow rejects is an error.
func CleanGeneratedImage(data []byte, mimeType string) (*ImageCleanResult, error) {
	var res *ImageCleanResult
	err := runImageClean(func() {
		switch mimeType {
		case "image/png":
			res = sanitizeGeneratedPNG(data)
		case "image/jpeg":
			res = sanitizeGeneratedJPEG(data)
		default:
			cleanFail("invalid_declared_format")
		}
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func imageCleanDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// bananaNative1K and normalized4K mirror BANANA_NATIVE_1K_DIMENSIONS (in
// Object.values order) and NORMALIZED_4K in lib/banana-image-dimensions.ts.
var bananaNative1K = [][2]float64{
	{1024, 1024}, {768, 1376}, {1376, 768}, {1200, 896}, {896, 1200},
	{1264, 848}, {848, 1264}, {1152, 928}, {928, 1152}, {1584, 672},
}

var normalized4K = [][2]float64{
	{4096, 4096}, {2304, 4096}, {4096, 2304}, {4096, 3072}, {3072, 4096},
	{4096, 2720}, {2720, 4096}, {4096, 3264}, {3264, 4096}, {4096, 1760},
}

func isRoundedGenerated4kDimensions(width, height float64) bool {
	for i, wh := range bananaNative1K {
		nw, nh := normalized4K[i][0], normalized4K[i][1]
		w4, h4 := wh[0]*4, wh[1]*4
		if width >= math.Ceil(math.Min(nw, w4)*0.95) && width <= math.Floor(math.Max(nw, w4)*1.05) &&
			height >= math.Ceil(math.Min(nh, h4)*0.95) && height <= math.Floor(math.Max(nh, h4)*1.05) {
			return true
		}
	}
	return false
}

// isSafeGeneratedImageDimensions mirrors lib/generated-image-validation.ts.
func isSafeGeneratedImageDimensions(width, height int64) bool {
	if width < 1 || height < 1 {
		return false
	}
	if width <= imageCleanMaxDimension && height <= imageCleanMaxDimension && width*height <= imageCleanMaxPixels {
		return true
	}
	return isRoundedGenerated4kDimensions(float64(width), float64(height))
}

// yeagrowImageAssetCleaner is the T2.5 implementation of the T2.4 call site.
func yeagrowImageAssetCleaner(_ context.Context, data []byte, mimeType string) (*ImageCleanResult, error) {
	if mimeType != "image/png" && mimeType != "image/jpeg" {
		return nil, fmt.Errorf("unsupported image type %q", mimeType)
	}
	return CleanGeneratedImage(data, mimeType)
}
