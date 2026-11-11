package service

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Golden samples exported from YeaGrow's own cleaners (see
// testdata/imageclean/export). Every input must give byte-identical output,
// digest and dimensions, or the same error code.

type goldenManifest struct {
	YeaGrowSHA string         `json:"yeagrow_sha"`
	Count      int            `json:"count"`
	Samples    []goldenSample `json:"samples"`
}

type goldenSample struct {
	ID          string       `json:"id"`
	Format      string       `json:"format"`
	External    bool         `json:"external_provenance"`
	Source      string       `json:"source"`
	Input       string       `json:"input"`
	InputSHA256 string       `json:"input_sha256"`
	InputSize   int          `json:"input_size"`
	Result      goldenResult `json:"result"`
}

type goldenResult struct {
	OK           bool    `json:"ok"`
	Code         string  `json:"code"`
	Output       *string `json:"output"`
	SHA256       string  `json:"sha256"`
	SourceSHA256 string  `json:"source_sha256"`
	MIMEType     string  `json:"mime_type"`
	Width        int     `json:"width"`
	Height       int     `json:"height"`
	ByteSize     int     `json:"byte_size"`
	Idempotent   bool    `json:"idempotent"`
}

// goldenDir can be pointed at an exploratory corpus (export.sh with
// GOLDEN_CORPUS_DIR and a large GOLDEN_MUTATIONS) via IMAGE_CLEAN_GOLDEN_DIR.
var goldenDir = func() string {
	if dir := os.Getenv("IMAGE_CLEAN_GOLDEN_DIR"); dir != "" {
		return dir
	}
	return "testdata/imageclean/corpus"
}()

func readGoldenBlob(t *testing.T, name string) []byte {
	t.Helper()
	f, err := os.Open(filepath.Join(goldenDir, name))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	data, err := io.ReadAll(zr)
	require.NoError(t, err)
	return data
}

func loadGoldenManifest(t *testing.T) goldenManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(goldenDir, "manifest.json"))
	require.NoError(t, err)
	var m goldenManifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.Equal(t, m.Count, len(m.Samples))
	return m
}

func TestImageCleanGoldenSamples(t *testing.T) {
	m := loadGoldenManifest(t)
	require.Equal(t, "215bd837d49297d7206246f60d37b8e5fdefe5e8", m.YeaGrowSHA)
	require.GreaterOrEqual(t, len(m.Samples), 300)
	mime := map[string]string{"png": "image/png", "jpeg": "image/jpeg"}
	accepted, rejected := 0, 0
	for _, s := range m.Samples {
		s := s
		t.Run(s.Format+"/"+s.ID, func(t *testing.T) {
			input := readGoldenBlob(t, s.Input)
			require.Equal(t, s.InputSize, len(input))
			require.Equal(t, s.InputSHA256, imageCleanDigest(input))
			res, err := CleanGeneratedImage(input, mime[s.Format])
			if !s.Result.OK {
				var cleanErr *ImageCleanError
				require.True(t, errors.As(err, &cleanErr), "%s: YeaGrow rejected (%s), Go accepted", s.Source, s.Result.Code)
				want := s.Result.Code
				if want == "!RangeError" {
					want = "range_error"
				}
				require.Equal(t, want, cleanErr.Code, "%s", s.Source)
				return
			}
			require.NoError(t, err, "%s", s.Source)
			want := input
			if s.Result.Output != nil {
				want = readGoldenBlob(t, *s.Result.Output)
			}
			require.True(t, bytes.Equal(want, res.Bytes), "%s: output bytes differ", s.Source)
			require.Equal(t, s.Result.SHA256, res.SHA256)
			require.Equal(t, s.Result.SourceSHA256, res.SourceSHA256)
			require.Equal(t, s.Result.MIMEType, res.MIMEType)
			require.Equal(t, s.Result.Width, res.Width)
			require.Equal(t, s.Result.Height, res.Height)
			require.Equal(t, s.Result.ByteSize, len(res.Bytes))
			require.Equal(t, s.Result.Output != nil, res.Changed)

			// YeaGrow's readback re-cleans the stored object and requires no change.
			require.True(t, s.Result.Idempotent)
			again, err := CleanGeneratedImage(res.Bytes, mime[s.Format])
			require.NoError(t, err)
			require.True(t, bytes.Equal(res.Bytes, again.Bytes))
			require.False(t, again.Changed)
		})
		if s.Result.OK {
			accepted++
		} else {
			rejected++
		}
	}
	t.Logf("golden samples: %d accepted, %d rejected (yeagrow %s)", accepted, rejected, m.YeaGrowSHA)
}

// The corpus must keep covering the categories the port depends on.
func TestImageCleanGoldenCoverage(t *testing.T) {
	m := loadGoldenManifest(t)
	codes := map[string]map[string]bool{"png": {}, "jpeg": {}}
	changed := map[string]int{}
	for _, s := range m.Samples {
		if s.Result.OK {
			codes[s.Format]["ok"] = true
			if s.Result.Output != nil {
				changed[s.Format]++
			}
		} else {
			codes[s.Format][s.Result.Code] = true
		}
	}
	for _, code := range []string{"ok", "image_too_large", "invalid_dimensions", "invalid_png_structure", "invalid_png_crc",
		"invalid_png_data", "invalid_metadata", "unsupported_metadata", "invalid_exif", "invalid_orientation", "invalid_color_profile",
		"metadata_too_large", "unsupported_png_chunk", "unsupported_png_animation", "duplicate_png_chunk", "invalid_png_chunk_order"} {
		require.True(t, codes["png"][code], "png corpus lacks %s", code)
	}
	for _, code := range []string{"ok", "image_too_large", "invalid_dimensions", "invalid_jpeg_structure", "invalid_metadata",
		"invalid_exif", "invalid_orientation", "invalid_color_profile", "unsupported_jpeg_app", "duplicate_jpeg_segment"} {
		require.True(t, codes["jpeg"][code], "jpeg corpus lacks %s", code)
	}
	require.Greater(t, changed["png"], 20)
	require.Greater(t, changed["jpeg"], 15)
}
