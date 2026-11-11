# Image-clean golden samples

`../corpus/` holds golden samples for the Go port of YeaGrow's generated-image
metadata cleaners (`service/image_clean*.go`). Every sample was produced by
**YeaGrow's own functions**, not by a re-implementation:

- `sanitizeGeneratedImageMetadata(bytes, "png")` — `lib/generated-image-metadata.ts`
- `sanitizeGeneratedJpegMetadata(bytes, "jpeg")` — `lib/generated-image-jpeg-metadata.ts`

Source: `finefu/yeagrow` @ `215bd837d49297d7206246f60d37b8e5fdefe5e8`
(`feat/t3-execution-plane-settle`), pako 2.1.0, Node 24. The SHA is recorded in
`corpus/manifest.json` and asserted by `TestImageCleanGoldenSamples`.

## How samples are collected

`hooks.mjs` (Node `module.registerHooks`) redirects every import of the two
cleaner modules to `wrap-png.mjs` / `wrap-jpeg.mjs`. The wrappers call the real
YeaGrow function, record input, output (or error code), SHA-256 values,
dimensions and whether re-cleaning the output is a no-op, and then return or
throw exactly what YeaGrow did. Inputs come from:

1. YeaGrow's own `tests/generated-image-metadata.test.ts` and
   `tests/generated-image-jpeg-metadata.test.ts` (all cases they exercise);
2. `extra-cases.test.mts`: Go stdlib encoder output (`seedgen/`), combined
   metadata (EXIF/XMP/C2PA caBX+dSIG/JUMBF/ICC/text/Photoshop/JFIF/Adobe/COM),
   truncation and corruption, dimension limits (4096, rounded 4K) and the
   10 MiB limit;
3. a deterministic mutation pass (byte flips, insertions, deletions,
   truncations; PNG mutations recompute the chunk CRC so they reach the
   DEFLATE decoder and metadata parsers).

Each record in `manifest.json` has the call site (`tests/<file>:<line>` or
`extra:<label>`), the gzipped input, and either the expected output
(`output: null` = unchanged) with digests and dimensions, or the YeaGrow error
code.

## Regenerate

```sh
# YeaGrow checkout at the pinned SHA with node_modules installed
./export.sh /path/to/yeagrow
# exploratory differential run (not committed)
GOLDEN_MUTATIONS=1500 GOLDEN_CORPUS_DIR=/tmp/explore ./export.sh /path/to/yeagrow
IMAGE_CLEAN_GOLDEN_DIR=/tmp/explore go test -run ImageCleanGolden ./internal/service
```

The script runs `seedgen` in a `golang:1.27.3` container and YeaGrow's tests
with `node --test --test-isolation=none --import hooks.mjs`. All tests must
pass; the wrappers do not change behaviour.
