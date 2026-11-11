package service

// Port of YeaGrow lib/generated-image-metadata.ts (profile png-capture-privacy-v2)
// @ finefu/yeagrow 215bd837d49297d7206246f60d37b8e5fdefe5e8. Function names and
// check order follow the TypeScript source so error codes match.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	pngMaxChunks           = 4096
	pngMaxMetadataBytes    = 2 * 1024 * 1024
	pngMaxInflatedMetadata = 1024 * 1024
	cleanMaxExifIFDs       = 64
	cleanMaxExifEntries    = 4096
)

var pngSignature = []byte{137, 80, 78, 71, 13, 10, 26, 10}

var (
	pngColorChannels      = map[byte]int{0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
	pngDepths             = map[byte][]byte{0: {1, 2, 4, 8, 16}, 2: {8, 16}, 3: {1, 2, 4, 8}, 4: {8, 16}, 6: {8, 16}}
	pngColorBeforePalette = setOf("cHRM", "gAMA", "iCCP", "sBIT", "sRGB", "cICP", "mDCV", "cLLI")
	pngBeforeIDAT         = setOf("cHRM", "gAMA", "iCCP", "sBIT", "sRGB", "cICP", "mDCV", "cLLI", "PLTE", "tRNS", "bKGD", "hIST", "pHYs", "sPLT", "eXIf")
	pngSingletons         = setOf("IHDR", "PLTE", "IEND", "cHRM", "gAMA", "iCCP", "sBIT", "sRGB", "cICP", "mDCV", "cLLI", "tRNS", "bKGD", "hIST", "pHYs", "eXIf", "tIME", "caBX")
	pngTextChunks         = setOf("tEXt", "zTXt", "iTXt")
	pngPrivateTextKeys    = setOf("author", "artist", "creationtime", "date", "datetime", "datetimeoriginal",
		"datetimedigitized", "gps", "gpslatitude", "gpslongitude", "gpsaltitude",
		"location", "latitude", "longitude", "serialnumber", "deviceserialnumber",
		"bodyserialnumber", "cameraserialnumber", "ownername", "cameraownername",
		"make", "model", "lensmake", "lensmodel", "lensserialnumber")
	pngSourceTextKeys = setOf("source", "software", "copyright", "ai", "aigc", "aigenerated", "digitalsourcetype")
	pngOtherAllowed   = setOf("IHDR", "caBX", "dSIG", "eXIf", "tIME", "sPLT", "tRNS", "bKGD", "hIST", "pHYs")
	pngFixedLengths   = map[string]int{"cHRM": 32, "gAMA": 4, "sRGB": 1, "cICP": 4, "mDCV": 24, "cLLI": 8, "pHYs": 9, "tIME": 7}
)

func setOf(items ...string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, it := range items {
		m[it] = true
	}
	return m
}

type pngChunk struct {
	typ  string
	data []byte
	raw  []byte
}

type pngHeader struct {
	width, height           int64
	depth, color, interlace byte
}

func pngCRC(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

func makePNGChunk(typ string, data []byte) []byte {
	out := make([]byte, len(data)+12)
	binary.BigEndian.PutUint32(out, uint32(len(data)))
	copy(out[4:], typ)
	copy(out[8:], data)
	binary.BigEndian.PutUint32(out[len(out)-4:], pngCRC(out[4:len(out)-4]))
	return out
}

func parsePNG(b []byte) ([]pngChunk, *pngHeader) {
	if len(b) < 8 || !bytes.Equal(b[:8], pngSignature) {
		cleanFail("image_format_mismatch")
	}
	var chunks []pngChunk
	seen := map[string]bool{}
	var header *pngHeader
	endedIDAT := false
	metadataBytes := 0
	paletteEntries := 0
	for offset := 8; offset < len(b); {
		if len(chunks) >= pngMaxChunks || len(b)-offset < 12 {
			cleanFail("invalid_png_structure")
		}
		length := int64(binary.BigEndian.Uint32(b[offset:]))
		if length > 0x7fffffff || length > int64(len(b)-offset-12) {
			cleanFail("invalid_png_structure")
		}
		end := offset + int(length) + 12
		name := b[offset+4 : offset+8]
		for _, c := range name {
			if (c < 65 || c > 90) && (c < 97 || c > 122) {
				cleanFail("invalid_png_structure")
			}
		}
		if name[2]&32 != 0 {
			cleanFail("invalid_png_structure")
		}
		typ := string(name)
		if pngCRC(b[offset+4:end-4]) != binary.BigEndian.Uint32(b[end-4:]) {
			cleanFail("invalid_png_crc")
		}
		if pngSingletons[typ] && seen[typ] {
			cleanFail("duplicate_png_chunk")
		}
		data := b[offset+8 : end-4]
		if typ != "IDAT" && typ != "caBX" && typ != "dSIG" {
			metadataBytes += int(length)
			if metadataBytes > pngMaxMetadataBytes {
				cleanFail("metadata_too_large")
			}
		}
		if header == nil {
			if typ != "IHDR" || offset != 8 || length != 13 {
				cleanFail("invalid_png_structure")
			}
			header = &pngHeader{
				width: int64(binary.BigEndian.Uint32(b[offset+8:])), height: int64(binary.BigEndian.Uint32(b[offset+12:])),
				depth: data[8], color: data[9], interlace: data[12],
			}
			if !isSafeGeneratedImageDimensions(header.width, header.height) {
				cleanFail("invalid_dimensions")
			}
			if !bytes.Contains(pngDepths[header.color], []byte{header.depth}) || data[10] != 0 || data[11] != 0 || header.interlace > 1 {
				cleanFail("invalid_png_structure")
			}
		} else if typ == "IHDR" {
			cleanFail("invalid_png_structure")
		}
		if seen["IDAT"] && typ != "IDAT" {
			endedIDAT = true
		}
		if (seen["IDAT"] && pngBeforeIDAT[typ]) || (seen["PLTE"] && pngColorBeforePalette[typ]) {
			cleanFail("invalid_png_chunk_order")
		}
		switch {
		case typ == "PLTE":
			if header.color == 0 || header.color == 4 || length < 3 || length > 768 || length%3 != 0 ||
				(header.color == 3 && length/3 > int64(1)<<header.depth) || seen["tRNS"] || seen["bKGD"] {
				cleanFail("invalid_png_structure")
			}
			paletteEntries = int(length / 3)
		case typ == "IDAT":
			if endedIDAT || (header.color == 3 && !seen["PLTE"]) {
				cleanFail("invalid_png_chunk_order")
			}
		case typ == "IEND":
			if length != 0 || !seen["IDAT"] || end != len(b) {
				cleanFail("invalid_png_structure")
			}
		case typ == "acTL" || typ == "fcTL" || typ == "fdAT":
			cleanFail("unsupported_png_animation")
		case !pngOtherAllowed[typ] && !pngTextChunks[typ] && !pngColorBeforePalette[typ]:
			cleanFail("unsupported_png_chunk")
		}
		validatePNGVisualChunk(typ, data, header, paletteEntries)
		if (typ == "caBX" || typ == "dSIG") && length == 0 {
			cleanFail("invalid_metadata")
		}
		seen[typ] = true
		chunks = append(chunks, pngChunk{typ: typ, data: data, raw: b[offset:end]})
		offset = end
	}
	if header == nil || len(chunks) == 0 || chunks[len(chunks)-1].typ != "IEND" {
		cleanFail("invalid_png_structure")
	}
	return chunks, header
}

func validatePNGVisualChunk(typ string, data []byte, header *pngHeader, paletteEntries int) {
	if fixed, ok := pngFixedLengths[typ]; ok && len(data) != fixed {
		cleanFail("invalid_png_structure")
	}
	maxSample := (1 << header.depth) - 1
	u16 := func(i int) int { return int(binary.BigEndian.Uint16(data[i:])) }
	switch typ {
	case "gAMA":
		if binary.BigEndian.Uint32(data) == 0 {
			cleanFail("invalid_png_structure")
		}
	case "sRGB":
		if data[0] > 3 {
			cleanFail("invalid_png_structure")
		}
	case "cICP":
		if data[2] != 0 || data[3] > 1 {
			cleanFail("invalid_png_structure")
		}
	case "pHYs":
		if data[8] > 1 {
			cleanFail("invalid_png_structure")
		}
	case "sBIT":
		channels := pngColorChannels[header.color]
		limit := header.depth
		if header.color == 3 {
			channels, limit = 3, 8
		}
		if len(data) != channels {
			cleanFail("invalid_png_structure")
		}
		for _, v := range data {
			if v == 0 || v > limit {
				cleanFail("invalid_png_structure")
			}
		}
	case "tRNS":
		if (header.color == 0 && len(data) != 2) || (header.color == 2 && len(data) != 6) ||
			(header.color == 3 && (paletteEntries == 0 || len(data) < 1 || len(data) > paletteEntries)) ||
			header.color == 4 || header.color == 6 {
			cleanFail("invalid_png_structure")
		}
		if header.color != 3 {
			for i := 0; i < len(data); i += 2 {
				if u16(i) > maxSample {
					cleanFail("invalid_png_structure")
				}
			}
		}
	case "bKGD":
		want := 6
		switch header.color {
		case 3:
			want = 1
		case 0, 4:
			want = 2
		}
		if len(data) != want || (header.color == 3 && (paletteEntries == 0 || int(data[0]) >= paletteEntries)) {
			cleanFail("invalid_png_structure")
		}
		if header.color != 3 {
			for i := 0; i < len(data); i += 2 {
				if u16(i) > maxSample {
					cleanFail("invalid_png_structure")
				}
			}
		}
	case "hIST":
		if paletteEntries == 0 || len(data) != paletteEntries*2 {
			cleanFail("invalid_png_structure")
		}
	case "tIME":
		if u16(0) == 0 || data[2] < 1 || data[2] > 12 || data[3] < 1 || data[3] > 31 || data[4] > 23 || data[5] > 59 || data[6] > 60 {
			cleanFail("invalid_metadata")
		}
	}
}

type cleanBudget struct{ remaining int }

func inflatePNGMetadata(data []byte, budget *cleanBudget) []byte {
	limit := pngMaxInflatedMetadata
	if budget.remaining < limit {
		limit = budget.remaining
	}
	var out []byte
	n := cleanInflate(data, limit, func(p []byte) { out = append(out, p...) }, "invalid_metadata", "metadata_too_large")
	budget.remaining -= n
	if out == nil {
		out = []byte{}
	}
	return out
}

// readPNGKeyword returns the Latin-1 keyword and the index of its NUL.
func readPNGKeyword(data []byte) (string, int) {
	end := bytes.IndexByte(data, 0)
	if end < 1 || end > 79 || data[0] == 32 || data[end-1] == 32 {
		cleanFail("invalid_metadata")
	}
	for i := 0; i < end; i++ {
		c := data[i]
		if (c < 32 || c > 126) && c < 161 || (c == 32 && i > 0 && data[i-1] == 32) {
			cleanFail("invalid_metadata")
		}
	}
	runes := make([]rune, end)
	for i := 0; i < end; i++ {
		runes[i] = rune(data[i])
	}
	return string(runes), end
}

func requireUTF8(b []byte) {
	if !utf8.Valid(b) {
		cleanFail("invalid_metadata")
	}
}

// JavaScript \s / String.prototype.trim whitespace (ECMAScript WhiteSpace + LineTerminator).
const jsSpaceClass = `\t\n\x{0b}\x{0c}\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}`

var (
	xmpPacketRe = regexp.MustCompile(`^[` + jsSpaceClass + `]*<x:xmpmeta([^<>]*)>[` + jsSpaceClass + `]*<rdf:RDF([^<>]*)>[` + jsSpaceClass +
		`]*<rdf:Description([^<>]*)/>[` + jsSpaceClass + `]*</rdf:RDF>[` + jsSpaceClass + `]*</x:xmpmeta>[` + jsSpaceClass + `]*$`)
	xmpAttributeRe = regexp.MustCompile(`^[` + jsSpaceClass + `]+([A-Za-z_][A-Za-z0-9_.:-]*)[` + jsSpaceClass + `]*=[` + jsSpaceClass +
		`]*(?:"([^"<&]*)"|'([^'<&]*)')`)
	xmpControlRe = regexp.MustCompile(`[\x00-\x08\x0b\x0c\x0e-\x1f]`)
)

func jsTrim(s string) string {
	return strings.TrimFunc(s, isJSSpace)
}

func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', 0x0b, 0x0c, '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

type orderedAttrs struct {
	keys   []string
	values map[string]string
}

func sourceXMPAttributes(source string) orderedAttrs {
	attrs := orderedAttrs{values: map[string]string{}}
	offset := 0
	for offset < len(source) {
		if jsTrim(source[offset:]) == "" {
			break
		}
		m := xmpAttributeRe.FindStringSubmatchIndex(source[offset:])
		if m == nil {
			cleanFail("unsupported_metadata")
		}
		name := source[offset+m[2] : offset+m[3]]
		value := ""
		if m[4] >= 0 {
			value = source[offset+m[4] : offset+m[5]]
		} else {
			value = source[offset+m[6] : offset+m[7]]
		}
		if _, dup := attrs.values[name]; dup || len(attrs.keys) >= 16 {
			cleanFail("unsupported_metadata")
		}
		attrs.keys = append(attrs.keys, name)
		attrs.values[name] = value
		offset += m[1]
	}
	return attrs
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n += len(utf16.Encode([]rune{r}))
	}
	return n
}

func validateSourceXMP(text []byte) {
	requireUTF8(text)
	source := string(bytes.TrimPrefix(text, []byte{0xef, 0xbb, 0xbf})) // TextDecoder drops a BOM
	if utf16Len(source) > 16_384 || xmpControlRe.MatchString(source) {
		cleanFail("unsupported_metadata")
	}
	packet := xmpPacketRe.FindStringSubmatch(source)
	if packet == nil {
		cleanFail("unsupported_metadata")
	}
	root := sourceXMPAttributes(packet[1])
	if v, ok := root.values["xmlns:x"]; !ok || v != "adobe:ns:meta/" {
		cleanFail("unsupported_metadata")
	}
	for _, k := range root.keys {
		if k != "xmlns:x" && k != "x:xmptk" {
			cleanFail("unsupported_metadata")
		}
	}
	namespaces := sourceXMPAttributes(packet[2])
	known := map[string]string{
		"xmlns:rdf":         "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		"xmlns:xmp":         "http://ns.adobe.com/xap/1.0/",
		"xmlns:dc":          "http://purl.org/dc/elements/1.1/",
		"xmlns:exif":        "http://ns.adobe.com/exif/1.0/",
		"xmlns:xml":         "http://www.w3.org/XML/1998/namespace",
		"xmlns:Iptc4xmpExt": "http://iptc.org/std/Iptc4xmpExt/2008-02-29/",
		"xmlns:photoshop":   "http://ns.adobe.com/photoshop/1.0/",
	}
	if v, ok := namespaces.values["xmlns:rdf"]; !ok || v != known["xmlns:rdf"] {
		cleanFail("unsupported_metadata")
	}
	for _, k := range namespaces.keys {
		if want, ok := known[k]; !ok || want != namespaces.values[k] {
			cleanFail("unsupported_metadata")
		}
	}
	declarations := sourceXMPAttributes(packet[3])
	if v, ok := declarations.values["rdf:about"]; !ok || v != "" || len(declarations.keys) < 2 {
		cleanFail("unsupported_metadata")
	}
	for _, k := range declarations.keys {
		v := declarations.values[k]
		switch k {
		case "rdf:about":
		case "Iptc4xmpExt:DigitalSourceType", "Iptc4xmpExt:DigitalSourceFileType":
			if _, ok := namespaces.values["xmlns:Iptc4xmpExt"]; !ok || v != "http://cv.iptc.org/newscodes/digitalsourcetype/trainedAlgorithmicMedia" {
				cleanFail("unsupported_metadata")
			}
		case "photoshop:Credit":
			if _, ok := namespaces.values["xmlns:photoshop"]; !ok || utf16Len(v) == 0 || utf16Len(v) > 1024 {
				cleanFail("unsupported_metadata")
			}
		default:
			cleanFail("unsupported_metadata")
		}
	}
}

// isLatin1Space: bytes that decode (windows-1252) to JavaScript \s.
func isLatin1Space(c byte) bool {
	return c == '\t' || c == '\n' || c == 0x0b || c == 0x0c || c == '\r' || c == ' ' || c == 0xa0
}

func validateSourceIPTC(text []byte) {
	// /^\s*IPTC profile\r?\n[ \t]*([0-9]{1,7})\r?\n([0-9a-fA-F\s]+)$/ over a windows-1252 decode.
	i := 0
	for i < len(text) && isLatin1Space(text[i]) {
		i++
	}
	if !bytes.HasPrefix(text[i:], []byte("IPTC profile")) {
		cleanFail("unsupported_metadata")
	}
	i += len("IPTC profile")
	newline := func() {
		if i < len(text) && text[i] == '\r' {
			i++
		}
		if i >= len(text) || text[i] != '\n' {
			cleanFail("unsupported_metadata")
		}
		i++
	}
	newline()
	for i < len(text) && (text[i] == ' ' || text[i] == '\t') {
		i++
	}
	digitsStart := i
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	if i-digitsStart < 1 || i-digitsStart > 7 {
		cleanFail("unsupported_metadata")
	}
	size, _ := strconv.Atoi(string(text[digitsStart:i]))
	newline()
	body := text[i:]
	if len(body) == 0 {
		cleanFail("unsupported_metadata")
	}
	var hexDigits []byte
	for _, c := range body {
		switch {
		case (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			hexDigits = append(hexDigits, c)
		case isLatin1Space(c):
		default:
			cleanFail("unsupported_metadata")
		}
	}
	if size == 0 || size > pngMaxInflatedMetadata || len(hexDigits) != size*2 {
		cleanFail("invalid_metadata")
	}
	data := make([]byte, size)
	for k := 0; k < size; k++ {
		v, _ := strconv.ParseUint(string(hexDigits[k*2:k*2+2]), 16, 8)
		data[k] = byte(v)
	}
	resources := map[int]bool{}
	offset := 0
	for offset < len(data) {
		if len(data)-offset < 12 || binary.BigEndian.Uint32(data[offset:]) != 0x3842494d || data[offset+6] != 0 || data[offset+7] != 0 {
			cleanFail("invalid_metadata")
		}
		resource := int(binary.BigEndian.Uint16(data[offset+4:]))
		length := int(binary.BigEndian.Uint32(data[offset+8:]))
		if resources[resource] || (resource != 0x0404 && resource != 0x0425) {
			cleanFail("unsupported_metadata")
		}
		resources[resource] = true
		offset += 12
		if length == 0 || length > len(data)-offset {
			cleanFail("invalid_metadata")
		}
		end := offset + length
		if resource == 0x0425 {
			if length != 16 {
				cleanFail("invalid_metadata")
			}
		} else {
			cursor := offset
			datasets := map[byte]bool{}
			for cursor < end {
				if end-cursor < 5 || data[cursor] != 0x1c || data[cursor+1] != 2 {
					cleanFail("invalid_metadata")
				}
				dataset := data[cursor+2]
				if (dataset != 110 && dataset != 116) || datasets[dataset] {
					cleanFail("unsupported_metadata")
				}
				datasets[dataset] = true
				count := int(binary.BigEndian.Uint16(data[cursor+3:]))
				cursor += 5
				if count == 0 || count > 1024 || count > end-cursor {
					cleanFail("invalid_metadata")
				}
				for _, c := range data[cursor : cursor+count] {
					if c < 32 || c > 126 {
						cleanFail("invalid_metadata")
					}
				}
				cursor += count
			}
		}
		offset = end
		if length%2 == 1 {
			// data[offset++] !== 0: out of range reads undefined, which is !== 0.
			if offset >= len(data) || data[offset] != 0 {
				cleanFail("invalid_metadata")
			}
			offset++
		}
	}
	if !resources[0x0404] {
		cleanFail("unsupported_metadata")
	}
}

// byteAt mirrors a JavaScript Uint8Array read: out of range is "undefined" (-1).
func byteAt(b []byte, i int) int {
	if i < 0 || i >= len(b) {
		return -1
	}
	return int(b[i])
}

func indexByteFrom(b []byte, c byte, from int) int {
	if from < 0 {
		from = 0
	}
	if from >= len(b) {
		return -1
	}
	if i := bytes.IndexByte(b[from:], c); i >= 0 {
		return from + i
	}
	return -1
}

// inspectPNGText returns true for "remove", false for "source".
func inspectPNGText(chunk pngChunk, budget *cleanBudget) bool {
	keyword, end := readPNGKeyword(chunk.data)
	var text []byte
	switch chunk.typ {
	case "tEXt":
		text = chunk.data[end+1:]
	case "zTXt":
		if byteAt(chunk.data, end+1) != 0 || end+2 >= len(chunk.data) {
			cleanFail("invalid_metadata")
		}
		text = inflatePNGMetadata(chunk.data[end+2:], budget)
	default: // iTXt
		flag := byteAt(chunk.data, end+1)
		if (flag != 0 && flag != 1) || byteAt(chunk.data, end+2) != 0 {
			cleanFail("invalid_metadata")
		}
		languageEnd := indexByteFrom(chunk.data, 0, end+3)
		translatedEnd := indexByteFrom(chunk.data, 0, languageEnd+1)
		if languageEnd < 0 || translatedEnd < 0 {
			cleanFail("invalid_metadata")
		}
		for _, c := range chunk.data[end+3 : languageEnd] {
			if c >= 128 {
				cleanFail("invalid_metadata")
			}
		}
		requireUTF8(chunk.data[languageEnd+1 : translatedEnd])
		if flag == 1 {
			text = inflatePNGMetadata(chunk.data[translatedEnd+1:], budget)
		} else {
			text = chunk.data[translatedEnd+1:]
		}
		requireUTF8(text)
	}
	if bytes.IndexByte(text, 0) >= 0 {
		cleanFail("invalid_metadata")
	}
	if keyword == "XML:com.adobe.xmp" {
		validateSourceXMP(text)
		return false
	}
	if keyword == "Raw profile type iptc" {
		validateSourceIPTC(text)
		return false
	}
	key := asciiLower(keyword)
	key = strings.NewReplacer(" ", "", "_", "", ":", "", "-", "").Replace(key)
	if pngPrivateTextKeys[key] {
		return true
	}
	if pngSourceTextKeys[key] {
		return false
	}
	cleanFail("unsupported_metadata")
	return false
}

// asciiLower lowercases A-Z only; a Latin-1 keyword can never lowercase into
// one of the ASCII private/source keys.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func validatePNGICC(data []byte, header *pngHeader, budget *cleanBudget) {
	_, end := readPNGKeyword(data)
	if byteAt(data, end+1) != 0 || end+2 >= len(data) {
		cleanFail("invalid_color_profile")
	}
	profile := inflatePNGMetadata(data[end+2:], budget)
	if len(profile) < 132 {
		cleanFail("invalid_color_profile")
	}
	name := func(o int) string { return string(profile[o : o+4]) }
	want := "RGB "
	if header.color == 0 || header.color == 4 {
		want = "GRAY"
	}
	if int(binary.BigEndian.Uint32(profile)) != len(profile) || name(36) != "acsp" || name(16) != want ||
		(name(20) != "XYZ " && name(20) != "Lab ") {
		cleanFail("invalid_color_profile")
	}
	count := int64(binary.BigEndian.Uint32(profile[128:]))
	if count*12 > int64(len(profile)-132) {
		cleanFail("invalid_color_profile")
	}
	tags := map[string]bool{}
	for i := int64(0); i < count; i++ {
		entry := 132 + int(i)*12
		tag := name(entry)
		offset := int64(binary.BigEndian.Uint32(profile[entry+4:]))
		size := int64(binary.BigEndian.Uint32(profile[entry+8:]))
		if tags[tag] || offset < 132+count*12 || size > int64(len(profile))-offset {
			cleanFail("invalid_color_profile")
		}
		tags[tag] = true
	}
}

// cleanEXIF is shared by PNG (eXIf, 1 MiB) and JPEG (APP1 Exif, 2 MiB). JPEG
// adds the safe-integer amount check and its own forbidden-tag error codes.
func cleanEXIF(data []byte, maxBytes int, jpeg bool) ([]byte, int) {
	if len(data) < 8 || len(data) > maxBytes {
		cleanFail("invalid_exif")
	}
	little := data[0] == 73 && data[1] == 73
	if !little && (data[0] != 77 || data[1] != 77) {
		cleanFail("invalid_exif")
	}
	var order binary.ByteOrder = binary.BigEndian
	if little {
		order = binary.LittleEndian
	}
	if order.Uint16(data[2:]) != 42 {
		cleanFail("invalid_exif")
	}
	type ifd struct {
		offset int64
		root   bool
	}
	pending := []ifd{{offset: int64(order.Uint32(data[4:])), root: true}}
	visited := map[int64]bool{}
	sizes := map[int]int64{1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8, 13: 4}
	n := int64(len(data))
	entries := 0
	orientation, other := 0, 0 // 0 = null
	for index := 0; index < len(pending); index++ {
		offset, root := pending[index].offset, pending[index].root
		if index >= cleanMaxExifIFDs || visited[offset] || offset < 8 || offset > n-6 {
			cleanFail("invalid_exif")
		}
		visited[offset] = true
		count := int64(order.Uint16(data[offset:]))
		entries += int(count)
		if entries > cleanMaxExifEntries || count*12 > n-offset-6 {
			cleanFail("invalid_exif")
		}
		seen := map[int]bool{}
		for entry := offset + 2; entry < offset+2+count*12; entry += 12 {
			tag := int(order.Uint16(data[entry:]))
			typ := int(order.Uint16(data[entry+2:]))
			amount := int64(order.Uint32(data[entry+4:]))
			unit, ok := sizes[typ]
			if seen[tag] || !ok || amount == 0 || (jpeg && amount > (1<<53-1)/unit) {
				cleanFail("invalid_exif")
			}
			seen[tag] = true
			size := amount * unit
			valueOffset := entry + 8
			if size > 4 {
				valueOffset = int64(order.Uint32(data[entry+8:]))
			}
			if valueOffset < 8 || size > n-valueOffset {
				cleanFail("invalid_exif")
			}
			if tag == 0x0112 {
				value := int(order.Uint16(data[valueOffset:]))
				if typ != 3 || amount != 1 || value < 1 || value > 8 || (other != 0 && other != value) {
					cleanFail("invalid_orientation")
				}
				other = value
				if root {
					orientation = value
				}
			}
			if jpeg {
				switch tag {
				case 0x8773, 0x013e, 0x013f, 0xa001, 0xa500, 0x012d, 0x0211, 0x0213, 0x0214:
					cleanFail("invalid_color_profile")
				case 0x0131, 0x8298, 0x02bc, 0x83bb:
					cleanFail("invalid_metadata")
				}
			} else {
				switch tag {
				case 0x0131, 0x8298, 0x02bc, 0x83bb, 0x8773, 0x013e, 0x013f:
					cleanFail("unsupported_metadata")
				}
			}
			if tag == 0x8769 || tag == 0x8825 || tag == 0xa005 || tag == 0x014a {
				if (typ != 4 && typ != 13) || (tag != 0x014a && amount != 1) || amount > int64(cleanMaxExifIFDs-len(pending)) {
					cleanFail("invalid_exif")
				}
				for item := int64(0); item < amount; item++ {
					target := int64(order.Uint32(data[valueOffset+item*4:]))
					if target != 0 {
						pending = append(pending, ifd{offset: target})
					}
				}
			}
		}
		next := int64(order.Uint32(data[offset+2+count*12:]))
		if next != 0 {
			pending = append(pending, ifd{offset: next})
		}
	}
	if orientation == 0 && other != 0 {
		cleanFail("invalid_orientation")
	}
	if orientation == 0 {
		return nil, 0
	}
	return minimalOrientationEXIF(orientation, little), orientation
}

func minimalOrientationEXIF(orientation int, little bool) []byte {
	if orientation < 1 || orientation > 8 {
		cleanFail("invalid_orientation")
	}
	out := make([]byte, 26)
	var order binary.ByteOrder = binary.BigEndian
	if little {
		out[0], out[1] = 73, 73
		order = binary.LittleEndian
	} else {
		out[0], out[1] = 77, 77
	}
	order.PutUint16(out[2:], 42)
	order.PutUint32(out[4:], 8)
	order.PutUint16(out[8:], 1)
	order.PutUint16(out[10:], 0x0112)
	order.PutUint16(out[12:], 3)
	order.PutUint32(out[14:], 1)
	order.PutUint16(out[18:], uint16(orientation))
	return out
}

func validatePNGPixels(chunks []pngChunk, header *pngHeader) {
	bits := int64(pngColorChannels[header.color]) * int64(header.depth)
	type pass struct{ rowBytes, rows int64 }
	var passes []pass
	patterns := [][4]int64{{0, 0, 1, 1}}
	if header.interlace != 0 {
		patterns = [][4]int64{{0, 0, 8, 8}, {4, 0, 8, 8}, {0, 4, 4, 8}, {2, 0, 4, 4}, {0, 2, 2, 4}, {1, 0, 2, 2}, {0, 1, 1, 2}}
	}
	ceilDiv := func(a, b int64) int64 {
		if a <= 0 {
			return 0
		}
		return (a + b - 1) / b
	}
	for _, p := range patterns {
		columns := ceilDiv(header.width-p[0], p[2])
		rows := ceilDiv(header.height-p[1], p[3])
		if columns != 0 && rows != 0 {
			passes = append(passes, pass{rowBytes: (columns*bits + 7) / 8, rows: rows})
		}
	}
	var expected int64
	for _, p := range passes {
		expected += (p.rowBytes + 1) * p.rows
	}
	passIndex := 0
	rows := passes[0].rows
	var remaining int64
	var idat []byte
	for _, c := range chunks {
		if c.typ == "IDAT" {
			idat = append(idat, c.data...)
		}
	}
	length := cleanInflate(idat, int(expected), func(part []byte) {
		for offset := int64(0); offset < int64(len(part)); {
			if passIndex >= len(passes) {
				cleanFail("invalid_png_data")
			}
			if remaining == 0 {
				if part[offset] > 4 {
					cleanFail("invalid_png_data")
				}
				offset++
				remaining = passes[passIndex].rowBytes
			}
			take := remaining
			if avail := int64(len(part)) - offset; avail < take {
				take = avail
			}
			remaining -= take
			offset += take
			if remaining == 0 {
				rows--
				if rows == 0 {
					passIndex++
					if passIndex < len(passes) {
						rows = passes[passIndex].rows
					} else {
						rows = 0
					}
				}
			}
		}
	}, "invalid_png_data", "invalid_png_data")
	if int64(length) != expected || remaining != 0 || passIndex != len(passes) {
		cleanFail("invalid_png_data")
	}
}

// sanitizeGeneratedPNG mirrors sanitizeGeneratedImageMetadata(bytes, "png").
func sanitizeGeneratedPNG(b []byte) *ImageCleanResult {
	if len(b) == 0 {
		cleanFail("invalid_png_structure")
	}
	if len(b) > imageCleanMaxBytes {
		cleanFail("image_too_large")
	}
	chunks, header := parsePNG(b)
	output := [][]byte{pngSignature}
	removed, rewritten := false, false
	budget := &cleanBudget{remaining: pngMaxMetadataBytes}
	paletteNames := map[string]bool{}
	for _, chunk := range chunks {
		replacement := chunk.raw
		keep := true
		switch {
		case chunk.typ == "caBX" || chunk.typ == "dSIG":
			keep = false
		case chunk.typ == "eXIf":
			clean, _ := cleanEXIF(chunk.data, pngMaxInflatedMetadata, false)
			if clean == nil {
				keep = false
			} else if !bytes.Equal(clean, chunk.data) {
				replacement = makePNGChunk("eXIf", clean)
				rewritten = true
			}
		case chunk.typ == "tIME":
			keep = false
		case pngTextChunks[chunk.typ]:
			if inspectPNGText(chunk, budget) {
				keep = false
			}
		case chunk.typ == "iCCP":
			validatePNGICC(chunk.data, header, budget)
		case chunk.typ == "sPLT":
			keyword, end := readPNGKeyword(chunk.data)
			depth := byteAt(chunk.data, end+1)
			entryBytes := 10
			if depth == 8 {
				entryBytes = 6
			}
			if paletteNames[keyword] || (depth != 8 && depth != 16) || len(chunk.data) <= end+2 || (len(chunk.data)-end-2)%entryBytes != 0 {
				cleanFail("invalid_metadata")
			}
			paletteNames[keyword] = true
		}
		if !keep {
			removed = true
		} else {
			output = append(output, replacement)
		}
	}
	changed := removed || rewritten
	validatePNGPixels(chunks, header)
	cleaned := b
	if changed {
		cleaned = bytes.Join(output, nil)
	}
	sum := imageCleanDigest(cleaned)
	source := sum
	if changed {
		source = imageCleanDigest(b)
	}
	return &ImageCleanResult{Bytes: cleaned, MIMEType: "image/png", Width: int(header.width), Height: int(header.height),
		SHA256: sum, SourceSHA256: source, Changed: changed}
}
