// Command seedgen writes realistic encoder output (Go stdlib PNG/JPEG
// encoders) used as extra golden-sample seeds. Deterministic: no randomness
// beyond a fixed LCG. Run: go run ./seedgen <dir>
package main

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
)

type lcg uint32

func (s *lcg) next() uint8 {
	*s = *s*1664525 + 1013904223
	return uint8(*s >> 24)
}

func photo(w, h int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	seed := lcg(42)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			n := seed.next() % 24
			img.Set(x, y, color.RGBA{R: uint8(x*255/w) + n, G: uint8(y*255/h) + n, B: uint8((x+y)*127/(w+h)) + n, A: 255})
		}
	}
	return img
}

func write(dir, name string, encode func(*bytes.Buffer) error) {
	var buf bytes.Buffer
	if err := encode(&buf); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o644); err != nil {
		panic(err)
	}
	fmt.Println(name, buf.Len())
}

func main() {
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	write(dir, "seed-photo-420.jpg", func(b *bytes.Buffer) error {
		return jpeg.Encode(b, photo(640, 480), &jpeg.Options{Quality: 90})
	})
	gray := image.NewGray(image.Rect(0, 0, 300, 200))
	for i := range gray.Pix {
		gray.Pix[i] = uint8(i * 7)
	}
	write(dir, "seed-gray.jpg", func(b *bytes.Buffer) error { return jpeg.Encode(b, gray, &jpeg.Options{Quality: 75}) })
	write(dir, "seed-photo.png", func(b *bytes.Buffer) error { return png.Encode(b, photo(256, 192)) })
	rgba64 := image.NewRGBA64(image.Rect(0, 0, 48, 40))
	for y := 0; y < 40; y++ {
		for x := 0; x < 48; x++ {
			rgba64.Set(x, y, color.RGBA64{R: uint16(x * 1300), G: uint16(y * 1500), B: 40000, A: 65535})
		}
	}
	write(dir, "seed-rgba64.png", func(b *bytes.Buffer) error { return png.Encode(b, rgba64) })
	palette := color.Palette{color.Black, color.White, color.RGBA{255, 0, 0, 255}, color.RGBA{0, 0, 255, 128}}
	paletted := image.NewPaletted(image.Rect(0, 0, 100, 60), palette)
	for i := range paletted.Pix {
		paletted.Pix[i] = uint8(i % 4)
	}
	write(dir, "seed-paletted.png", func(b *bytes.Buffer) error { return png.Encode(b, paletted) })
	write(dir, "seed-small.png", func(b *bytes.Buffer) error { return png.Encode(b, photo(24, 16)) })
	write(dir, "seed-small.jpg", func(b *bytes.Buffer) error { return jpeg.Encode(b, photo(48, 32), &jpeg.Options{Quality: 80}) })
	write(dir, "seed-best.png", func(b *bytes.Buffer) error {
		enc := png.Encoder{CompressionLevel: png.BestCompression}
		return enc.Encode(b, photo(200, 150))
	})
}
