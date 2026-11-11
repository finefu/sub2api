package service

// Port of YeaGrow lib/generated-image-jpeg-metadata.ts (profile jpeg-capture-privacy-v1)
// @ finefu/yeagrow 215bd837d49297d7206246f60d37b8e5fdefe5e8. Entropy-coded data
// is validated but never re-encoded. Check order follows the TypeScript source.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"bytes"
	"encoding/binary"
	"regexp"
	"sort"
	"strconv"
)

const (
	jpegMaxMetadataBytes  = 2 * 1024 * 1024
	jpegMaxSegments       = 8192
	jpegMaxScans          = 1024
	jpegMaxNestedJFXX     = 1
	jpegMaxCoefficientMem = 16 * 1024 * 1024
	jpegMaxEntropyWork    = 128 * 1024 * 1024
)

var (
	jpegExifPrefix        = []byte("Exif\x00\x00")
	jpegXMPPrefix         = []byte("http://ns.adobe.com/xap/1.0/\x00")
	jpegXMPExtendedPrefix = []byte("http://ns.adobe.com/xmp/extension/\x00")
	jpegICCPrefix         = []byte("ICC_PROFILE\x00")
	jpegJFIFPrefix        = []byte("JFIF\x00")
	jpegJFXXPrefix        = []byte("JFXX\x00")
	jpegPhotoshopPrefix   = []byte("Photoshop 3.0\x00")

	jpegOrientationRe = regexp.MustCompile(`orientation`)
	jpegColorWordsRe  = regexp.MustCompile(`\b(?:iccprofile|colorspace|whitepoint|primarychromaticities|transferfunction|ycbcrcoefficients|referenceblackwhite)\b`)
	jpegGUIDRe        = regexp.MustCompile(`^[0-9A-F]{32}$`)
)

type jpegFrameComponent struct {
	id, horizontal, vertical, quantizationTable            int
	paddedBlocksWide, visibleBlocksWide, visibleBlocksHigh int
}

type jpegFrame struct {
	marker, precision, width, height   int
	componentIDs                       []int
	components                         map[int]*jpegFrameComponent
	quantizationTables                 []int
	maximumHorizontal, maximumVertical int
	mcuColumns, mcuRows                int
}

type jpegHuffman struct {
	minimum, maximum, valueOffset [17]int
	symbols                       []byte
}

type jpegScanComponent struct {
	component        *jpegFrameComponent
	dcTable, acTable int
}

type jpegScan struct {
	components            []jpegScanComponent
	start, end, high, low int
}

type jpegPendingScan struct {
	raw             []byte
	scan            *jpegScan
	huffman         map[string]*jpegHuffman
	restartInterval int
}

type jpegICCPart struct {
	sequence, count int
	data            []byte
}

type jpegExtXMPPart struct {
	guid               string
	fullLength, offset int64
	data               []byte
}

type jpegParseResult struct {
	bytes         []byte
	changed       bool
	width, height int
}

func jpegValidateUTF8(b []byte) string {
	if len(b) == 0 || bytes.IndexByte(b, 0) >= 0 {
		cleanFail("invalid_metadata")
	}
	requireUTF8(b)
	return string(bytes.TrimPrefix(b, []byte{0xef, 0xbb, 0xbf}))
}

func validateRemovableXMP(b []byte) {
	source := asciiLower(jpegValidateUTF8(b)) // regexes are ASCII case-insensitive
	if jpegOrientationRe.MatchString(source) {
		cleanFail("invalid_orientation")
	}
	if jpegColorWordsRe.MatchString(source) {
		cleanFail("invalid_color_profile")
	}
}

func makeJPEGSegment(marker byte, payload []byte) []byte {
	if len(payload) > 65_533 {
		cleanFail("invalid_metadata")
	}
	out := make([]byte, len(payload)+4)
	out[0], out[1] = 0xff, marker
	binary.BigEndian.PutUint16(out[2:], uint16(len(payload)+2))
	copy(out[4:], payload)
	return out
}

func cleanJFIF(payload []byte) []byte {
	if len(payload) < 14 || !bytes.HasPrefix(payload, jpegJFIFPrefix) {
		cleanFail("invalid_metadata")
	}
	if payload[5] != 1 || payload[6] > 2 || payload[7] > 2 {
		cleanFail("invalid_metadata")
	}
	if binary.BigEndian.Uint16(payload[8:]) == 0 || binary.BigEndian.Uint16(payload[10:]) == 0 {
		cleanFail("invalid_metadata")
	}
	thumbnail := int(payload[12]) * int(payload[13]) * 3
	if len(payload) != 14+thumbnail {
		cleanFail("invalid_metadata")
	}
	if thumbnail == 0 {
		return payload
	}
	clean := append([]byte(nil), payload[:14]...)
	clean[12], clean[13] = 0, 0
	return clean
}

func validateJFXX(payload []byte, depth int, work *int64) {
	if len(payload) < 8 || !bytes.HasPrefix(payload, jpegJFXXPrefix) {
		cleanFail("invalid_metadata")
	}
	code, width, height := payload[5], int(payload[6]), int(payload[7])
	if code == 0x10 {
		if depth >= jpegMaxNestedJFXX || len(payload) < 10 {
			cleanFail("invalid_metadata")
		}
		parseJPEG(payload[6:], depth+1, work)
		return
	}
	if width == 0 || height == 0 {
		cleanFail("invalid_metadata")
	}
	switch code {
	case 0x11:
		if len(payload) != 8+768+width*height {
			cleanFail("invalid_metadata")
		}
	case 0x13:
		if len(payload) != 8+width*height*3 {
			cleanFail("invalid_metadata")
		}
	default:
		cleanFail("invalid_metadata")
	}
}

func validateAdobe(payload []byte) {
	if len(payload) != 12 || string(payload[:5]) != "Adobe" {
		cleanFail("invalid_metadata")
	}
	if binary.BigEndian.Uint16(payload[5:]) == 0 || payload[11] > 2 {
		cleanFail("invalid_metadata")
	}
}

func cleanPhotoshop(payload []byte) (data, icc []byte) {
	if !bytes.HasPrefix(payload, jpegPhotoshopPrefix) || len(payload) == len(jpegPhotoshopPrefix) {
		cleanFail("invalid_metadata")
	}
	n := len(payload)
	offset := len(jpegPhotoshopPrefix)
	blocks := 0
	for offset < n {
		blocks++
		if blocks > 4096 || n-offset < 12 || string(payload[offset:offset+4]) != "8BIM" {
			cleanFail("invalid_metadata")
		}
		resourceID := binary.BigEndian.Uint16(payload[offset+4:])
		offset += 6
		nameLength := int(payload[offset])
		if nameLength > n-offset-1 {
			cleanFail("invalid_metadata")
		}
		offset += 1 + nameLength
		if (1+nameLength)%2 == 1 {
			offset++
		}
		if n-offset < 4 {
			cleanFail("invalid_metadata")
		}
		size := int64(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		if size > int64(n-offset) {
			cleanFail("invalid_metadata")
		}
		block := payload[offset : offset+int(size)]
		if resourceID == 0x040f {
			if icc != nil || len(block) == 0 {
				cleanFail("invalid_color_profile")
			}
			icc = block
		}
		offset += int(size)
		if size%2 == 1 {
			offset++
		}
		if offset > n {
			cleanFail("invalid_metadata")
		}
	}
	if offset != n {
		cleanFail("invalid_metadata")
	}
	if icc == nil {
		return nil, nil
	}
	block := make([]byte, 12+len(icc)+len(icc)%2)
	copy(block, []byte{0x38, 0x42, 0x49, 0x4d, 0x04, 0x0f, 0, 0})
	binary.BigEndian.PutUint32(block[8:], uint32(len(icc)))
	copy(block[12:], icc)
	return append(append([]byte(nil), jpegPhotoshopPrefix...), block...), icc
}

func validateDQT(payload []byte, tables map[int]bool) {
	offset := 0
	declared := map[int]bool{}
	for offset < len(payload) {
		descriptor := payload[offset]
		offset++
		precision, table := int(descriptor>>4), int(descriptor&0x0f)
		if precision > 1 || table > 3 || declared[table] {
			cleanFail("invalid_jpeg_structure")
		}
		size := 64
		if precision != 0 {
			size = 128
		}
		if size > len(payload)-offset {
			cleanFail("invalid_jpeg_structure")
		}
		for i := offset; i < offset+size; i += precision + 1 {
			value := int(payload[i])
			if precision != 0 {
				value = value<<8 | int(payload[i+1])
			}
			if value == 0 {
				cleanFail("invalid_jpeg_structure")
			}
		}
		declared[table] = true
		tables[table] = true
		offset += size
	}
	if offset == 0 {
		cleanFail("invalid_jpeg_structure")
	}
}

func compileJPEGHuffman(counts, symbols []byte) *jpegHuffman {
	h := &jpegHuffman{}
	for i := range h.minimum {
		h.minimum[i], h.maximum[i] = -1, -1
	}
	code, symbolOffset := 0, 0
	for length := 1; length <= 16; length++ {
		count := int(counts[length-1])
		if count > 0 {
			maximum := code + count - 1
			if maximum >= (1<<length)-1 {
				cleanFail("invalid_jpeg_structure")
			}
			h.minimum[length], h.maximum[length], h.valueOffset[length] = code, maximum, symbolOffset
			symbolOffset += count
			code += count
		}
		code *= 2
		if code > 1<<(length+1) {
			cleanFail("invalid_jpeg_structure")
		}
	}
	if symbolOffset != len(symbols) {
		cleanFail("invalid_jpeg_structure")
	}
	h.symbols = append([]byte(nil), symbols...)
	return h
}

func validateDHT(payload []byte, tables map[string]*jpegHuffman) {
	offset := 0
	declared := map[string]bool{}
	for offset < len(payload) {
		if len(payload)-offset < 17 {
			cleanFail("invalid_jpeg_structure")
		}
		descriptor := payload[offset]
		offset++
		class, table := int(descriptor>>4), int(descriptor&0x0f)
		key := strconv.Itoa(class) + ":" + strconv.Itoa(table)
		if class > 1 || table > 3 || declared[key] {
			cleanFail("invalid_jpeg_structure")
		}
		counts := payload[offset : offset+16]
		symbols := 0
		for _, c := range counts {
			symbols += int(c)
		}
		offset += 16
		if symbols == 0 || symbols > 256 || symbols > len(payload)-offset {
			cleanFail("invalid_jpeg_structure")
		}
		declared[key] = true
		tables[key] = compileJPEGHuffman(counts, payload[offset:offset+symbols])
		offset += symbols
	}
	if offset == 0 {
		cleanFail("invalid_jpeg_structure")
	}
}

func ceilDivInt(a, b int) int { return (a + b - 1) / b }

func parseJPEGFrame(marker int, payload []byte) *jpegFrame {
	if len(payload) < 9 {
		cleanFail("invalid_jpeg_structure")
	}
	precision := int(payload[0])
	height := int(binary.BigEndian.Uint16(payload[1:]))
	width := int(binary.BigEndian.Uint16(payload[3:]))
	count := int(payload[5])
	if !isSafeGeneratedImageDimensions(int64(width), int64(height)) {
		cleanFail("invalid_dimensions")
	}
	if precision != 8 || (count != 1 && count != 3) || len(payload) != 6+count*3 {
		cleanFail("invalid_jpeg_structure")
	}
	f := &jpegFrame{marker: marker, precision: precision, width: width, height: height, components: map[int]*jpegFrameComponent{}}
	var parsed []*jpegFrameComponent
	samplingUnits := 0
	for offset := 6; offset < len(payload); offset += 3 {
		id := int(payload[offset])
		h, v := int(payload[offset+1]>>4), int(payload[offset+1]&0x0f)
		q := int(payload[offset+2])
		for _, existing := range f.componentIDs {
			if existing == id {
				cleanFail("invalid_jpeg_structure")
			}
		}
		if h < 1 || h > 4 || v < 1 || v > 4 || q > 3 {
			cleanFail("invalid_jpeg_structure")
		}
		f.componentIDs = append(f.componentIDs, id)
		f.quantizationTables = append(f.quantizationTables, q)
		samplingUnits += h * v
		if h > f.maximumHorizontal {
			f.maximumHorizontal = h
		}
		if v > f.maximumVertical {
			f.maximumVertical = v
		}
		parsed = append(parsed, &jpegFrameComponent{id: id, horizontal: h, vertical: v, quantizationTable: q})
	}
	if samplingUnits > 10 {
		cleanFail("invalid_jpeg_structure")
	}
	f.mcuColumns = ceilDivInt(width, 8*f.maximumHorizontal)
	f.mcuRows = ceilDivInt(height, 8*f.maximumVertical)
	for _, c := range parsed {
		c.paddedBlocksWide = f.mcuColumns * c.horizontal
		c.visibleBlocksWide = ceilDivInt(width*c.horizontal, 8*f.maximumHorizontal)
		c.visibleBlocksHigh = ceilDivInt(height*c.vertical, 8*f.maximumVertical)
		f.components[c.id] = c
	}
	return f
}

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func progKey(component, coefficient int) string {
	return strconv.Itoa(component) + ":" + strconv.Itoa(coefficient)
}

func validateSOS(payload []byte, f *jpegFrame, tables map[string]*jpegHuffman, scanned map[int]bool, progressive map[string]int) *jpegScan {
	if len(payload) < 6 {
		cleanFail("invalid_jpeg_structure")
	}
	count := int(payload[0])
	if count < 1 || count > len(f.componentIDs) || len(payload) != 1+count*2+3 {
		cleanFail("invalid_jpeg_structure")
	}
	seen := map[int]bool{}
	var seenOrder []int
	var components []jpegScanComponent
	for offset := 1; offset < 1+count*2; offset += 2 {
		component := int(payload[offset])
		dc, ac := int(payload[offset+1]>>4), int(payload[offset+1]&0x0f)
		if !containsInt(f.componentIDs, component) || seen[component] || dc > 3 || ac > 3 {
			cleanFail("invalid_jpeg_structure")
		}
		if f.marker != 0xc2 && scanned[component] {
			cleanFail("invalid_jpeg_structure")
		}
		seen[component] = true
		seenOrder = append(seenOrder, component)
		components = append(components, jpegScanComponent{component: f.components[component], dcTable: dc, acTable: ac})
	}
	tail := 1 + count*2
	start, end, approx := int(payload[tail]), int(payload[tail+1]), int(payload[tail+2])
	if start > end || end > 63 || approx>>4 > 13 || approx&0x0f > 13 {
		cleanFail("invalid_jpeg_structure")
	}
	high, low := approx>>4, approx&0x0f
	if f.marker != 0xc2 && !(start == 0 && end == 63 && approx == 0) {
		cleanFail("invalid_jpeg_structure")
	}
	if f.marker == 0xc2 {
		if (start == 0 && end != 0) || (start > 0 && count != 1) {
			cleanFail("invalid_jpeg_structure")
		}
		if high != 0 && high != low+1 {
			cleanFail("invalid_jpeg_structure")
		}
		if start > 0 {
			for _, c := range seenOrder {
				if _, ok := progressive[progKey(c, 0)]; !ok {
					cleanFail("invalid_jpeg_structure")
				}
			}
		}
		for _, c := range seenOrder {
			for coefficient := start; coefficient <= end; coefficient++ {
				key := progKey(c, coefficient)
				previous, has := progressive[key]
				if high == 0 {
					if has {
						cleanFail("invalid_jpeg_structure")
					}
				} else if !has || previous != high {
					cleanFail("invalid_jpeg_structure")
				}
				progressive[key] = low
			}
		}
	}
	for _, c := range components {
		_, hasDC := tables["0:"+strconv.Itoa(c.dcTable)]
		_, hasAC := tables["1:"+strconv.Itoa(c.acTable)]
		if f.marker != 0xc2 {
			if !hasDC || !hasAC {
				cleanFail("invalid_jpeg_structure")
			}
		} else if start == 0 {
			if c.acTable != 0 || (high == 0 && !hasDC) || (high > 0 && c.dcTable != 0) {
				cleanFail("invalid_jpeg_structure")
			}
		} else if c.dcTable != 0 || !hasAC {
			cleanFail("invalid_jpeg_structure")
		}
	}
	for _, c := range seenOrder {
		scanned[c] = true
	}
	return &jpegScan{components: components, start: start, end: end, high: high, low: low}
}

type jpegEntropyReader struct {
	raw       []byte
	offset    int
	current   int
	remaining int
}

func (r *jpegEntropyReader) readDataByte() int {
	if r.offset >= len(r.raw) {
		cleanFail("invalid_jpeg_structure")
	}
	v := r.raw[r.offset]
	r.offset++
	if v != 0xff {
		return int(v)
	}
	if r.offset >= len(r.raw) || r.raw[r.offset] != 0x00 {
		r.offset--
		cleanFail("invalid_jpeg_structure")
	}
	r.offset++
	return 0xff
}

func (r *jpegEntropyReader) readBit() int {
	if r.remaining == 0 {
		r.current = r.readDataByte()
		r.remaining = 8
	}
	r.remaining--
	return (r.current >> r.remaining) & 1
}

func (r *jpegEntropyReader) readBits(count int) int {
	if count < 0 || count > 16 {
		cleanFail("invalid_jpeg_structure")
	}
	v := 0
	for i := 0; i < count; i++ {
		v = v*2 + r.readBit()
	}
	return v
}

func (r *jpegEntropyReader) alignWithOnePadding() {
	if r.remaining > 0 {
		mask := (1 << r.remaining) - 1
		if r.current&mask != mask {
			cleanFail("invalid_jpeg_structure")
		}
		r.remaining = 0
	}
}

func (r *jpegEntropyReader) consumeRestart(expected int) {
	r.alignWithOnePadding()
	if r.offset >= len(r.raw) || r.raw[r.offset] != 0xff {
		cleanFail("invalid_jpeg_structure")
	}
	r.offset++
	for r.offset < len(r.raw) && r.raw[r.offset] == 0xff {
		r.offset++
	}
	if r.offset >= len(r.raw) || int(r.raw[r.offset]) != 0xd0+expected {
		cleanFail("invalid_jpeg_structure")
	}
	r.offset++
}

func (r *jpegEntropyReader) finish() {
	r.alignWithOnePadding()
	if r.offset != len(r.raw) {
		cleanFail("invalid_jpeg_structure")
	}
}

func decodeJPEGHuffman(r *jpegEntropyReader, t *jpegHuffman) int {
	if t == nil {
		cleanFail("invalid_jpeg_structure")
	}
	code := 0
	for length := 1; length <= 16; length++ {
		code = code*2 + r.readBit()
		minimum := t.minimum[length]
		if minimum >= 0 && code >= minimum && code <= t.maximum[length] {
			return int(t.symbols[t.valueOffset[length]+code-minimum])
		}
	}
	cleanFail("invalid_jpeg_structure")
	return 0
}

type jpegCoefficients map[int][]uint32

func newProgressiveCoefficients(f *jpegFrame) jpegCoefficients {
	state := jpegCoefficients{}
	total := 0
	// Map iteration order in JavaScript is insertion (component) order.
	for _, id := range f.componentIDs {
		c := f.components[id]
		blocks := c.paddedBlocksWide * f.mcuRows * c.vertical
		total += blocks * 2 * 4
		if total > jpegMaxCoefficientMem {
			cleanFail("invalid_dimensions")
		}
		state[id] = make([]uint32, blocks*2)
	}
	return state
}

func (s jpegCoefficients) nonzero(component, block, coefficient int) bool {
	words, ok := s[component]
	if !ok {
		cleanFail("invalid_jpeg_structure")
	}
	return words[block*2+(coefficient>>5)]&(1<<(coefficient&31)) != 0
}

func (s jpegCoefficients) mark(component, block, coefficient int) {
	words, ok := s[component]
	if !ok {
		cleanFail("invalid_jpeg_structure")
	}
	words[block*2+(coefficient>>5)] |= 1 << (coefficient & 31)
}

func jpegScanMCUCount(f *jpegFrame, s *jpegScan) int {
	if len(s.components) > 1 {
		return f.mcuColumns * f.mcuRows
	}
	c := s.components[0].component
	return c.visibleBlocksWide * c.visibleBlocksHigh
}

func chargeJPEGEntropyWork(work *int64, f *jpegFrame, pending []jpegPendingScan) {
	for _, p := range pending {
		mcus := int64(jpegScanMCUCount(f, p.scan))
		blocksPerMCU := int64(1)
		if len(p.scan.components) != 1 {
			blocksPerMCU = 0
			for _, c := range p.scan.components {
				blocksPerMCU += int64(c.component.horizontal * c.component.vertical)
			}
		}
		blocks := mcus * blocksPerMCU
		coefficients := int64(64)
		if f.marker == 0xc2 {
			coefficients = int64(p.scan.end - p.scan.start + 1)
		}
		units := mcus + blocks + blocks*coefficients
		if units < 1 || units > *work {
			cleanFail("invalid_jpeg_structure")
		}
		*work -= units
	}
}

func forEachJPEGMCUBlock(f *jpegFrame, s *jpegScan, mcu int, callback func(c jpegScanComponent, block int)) {
	if len(s.components) == 1 {
		c := s.components[0]
		x := mcu % c.component.visibleBlocksWide
		y := mcu / c.component.visibleBlocksWide
		callback(c, y*c.component.paddedBlocksWide+x)
		return
	}
	mcuX, mcuY := mcu%f.mcuColumns, mcu/f.mcuColumns
	for _, c := range s.components {
		for v := 0; v < c.component.vertical; v++ {
			for h := 0; h < c.component.horizontal; h++ {
				x := mcuX*c.component.horizontal + h
				y := mcuY*c.component.vertical + v
				callback(c, y*c.component.paddedBlocksWide+x)
			}
		}
	}
}

func decodeJPEGSequentialBlock(r *jpegEntropyReader, f *jpegFrame, c jpegScanComponent, tables map[string]*jpegHuffman) {
	dcSize := decodeJPEGHuffman(r, tables["0:"+strconv.Itoa(c.dcTable)])
	if dcSize > f.precision+3 {
		cleanFail("invalid_jpeg_structure")
	}
	r.readBits(dcSize)
	ac := tables["1:"+strconv.Itoa(c.acTable)]
	for coefficient := 1; coefficient <= 63; {
		symbol := decodeJPEGHuffman(r, ac)
		run, size := symbol>>4, symbol&0x0f
		if size == 0 {
			if run == 0 {
				return
			}
			if run != 15 || coefficient+16 > 64 {
				cleanFail("invalid_jpeg_structure")
			}
			coefficient += 16
			continue
		}
		if size > 10 { // precision is always 8 here
			cleanFail("invalid_jpeg_structure")
		}
		coefficient += run
		if coefficient > 63 {
			cleanFail("invalid_jpeg_structure")
		}
		r.readBits(size)
		coefficient++
	}
}

func decodeJPEGProgressiveBlock(r *jpegEntropyReader, f *jpegFrame, s *jpegScan, c jpegScanComponent, block int,
	tables map[string]*jpegHuffman, coefficients jpegCoefficients, eobRun *int) {
	id := c.component.id
	if s.start == 0 {
		if s.high == 0 {
			size := decodeJPEGHuffman(r, tables["0:"+strconv.Itoa(c.dcTable)])
			if size > f.precision+3 {
				cleanFail("invalid_jpeg_structure")
			}
			r.readBits(size)
			if size > 0 {
				coefficients.mark(id, block, 0)
			}
		} else if r.readBit() == 1 {
			coefficients.mark(id, block, 0)
		}
		return
	}
	ac := tables["1:"+strconv.Itoa(c.acTable)]
	if s.high == 0 {
		if *eobRun > 0 {
			*eobRun--
			return
		}
		for coefficient := s.start; coefficient <= s.end; {
			symbol := decodeJPEGHuffman(r, ac)
			run, size := symbol>>4, symbol&0x0f
			if size == 0 {
				if run == 15 {
					if coefficient+16 > s.end+1 {
						cleanFail("invalid_jpeg_structure")
					}
					coefficient += 16
					continue
				}
				extra := 0
				if run > 0 {
					extra = r.readBits(run)
				}
				*eobRun = (1 << run) + extra - 1
				return
			}
			if size > 10 {
				cleanFail("invalid_jpeg_structure")
			}
			coefficient += run
			if coefficient > s.end {
				cleanFail("invalid_jpeg_structure")
			}
			r.readBits(size)
			coefficients.mark(id, block, coefficient)
			coefficient++
		}
		return
	}
	coefficient := s.start
	if *eobRun == 0 {
		for coefficient <= s.end {
			symbol := decodeJPEGHuffman(r, ac)
			run, size := symbol>>4, symbol&0x0f
			add := false
			if size == 1 {
				r.readBit()
				add = true
			} else if size != 0 {
				cleanFail("invalid_jpeg_structure")
			} else if run != 15 {
				extra := 0
				if run > 0 {
					extra = r.readBits(run)
				}
				*eobRun = (1 << run) + extra
				break
			} else {
				run = 16
			}
			for coefficient <= s.end {
				if coefficients.nonzero(id, block, coefficient) {
					r.readBit()
				} else {
					if run == 0 {
						break
					}
					run--
				}
				coefficient++
			}
			if add {
				if coefficient > s.end {
					cleanFail("invalid_jpeg_structure")
				}
				coefficients.mark(id, block, coefficient)
				coefficient++
			} else if run != 0 {
				cleanFail("invalid_jpeg_structure")
			}
		}
	}
	if *eobRun > 0 {
		for ; coefficient <= s.end; coefficient++ {
			if coefficients.nonzero(id, block, coefficient) {
				r.readBit()
			}
		}
		*eobRun--
	}
}

func validateJPEGEntropyScan(raw []byte, f *jpegFrame, s *jpegScan, tables map[string]*jpegHuffman, restartInterval int, coefficients jpegCoefficients) {
	r := &jpegEntropyReader{raw: raw}
	count := jpegScanMCUCount(f, s)
	eobRun := 0
	for mcu := 0; mcu < count; mcu++ {
		if mcu > 0 && restartInterval > 0 && mcu%restartInterval == 0 {
			if eobRun != 0 {
				cleanFail("invalid_jpeg_structure")
			}
			r.consumeRestart((mcu/restartInterval - 1) % 8)
		}
		forEachJPEGMCUBlock(f, s, mcu, func(c jpegScanComponent, block int) {
			if f.marker == 0xc2 {
				decodeJPEGProgressiveBlock(r, f, s, c, block, tables, coefficients, &eobRun)
			} else {
				decodeJPEGSequentialBlock(r, f, c, tables)
			}
		})
	}
	if eobRun != 0 {
		cleanFail("invalid_jpeg_structure")
	}
	r.finish()
}

func validateJPEGICCProfile(profile []byte, f *jpegFrame) {
	if len(profile) < 132 || len(profile) > jpegMaxMetadataBytes {
		cleanFail("invalid_color_profile")
	}
	name := func(o int) string { return string(profile[o : o+4]) }
	if int(binary.BigEndian.Uint32(profile)) != len(profile) || name(36) != "acsp" || (name(20) != "XYZ " && name(20) != "Lab ") {
		cleanFail("invalid_color_profile")
	}
	expected := "CMYK"
	if len(f.componentIDs) == 1 {
		expected = "GRAY"
	} else if len(f.componentIDs) == 3 {
		expected = "RGB "
	}
	if name(16) != expected {
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
		if tags[tag] || offset < 132+count*12 || offset%4 != 0 || size > int64(len(profile))-offset {
			cleanFail("invalid_color_profile")
		}
		tags[tag] = true
	}
}

func validateJPEGICC(parts []jpegICCPart, f *jpegFrame) []byte {
	if len(parts) == 0 {
		return nil
	}
	count := parts[0].count
	if count < 1 || len(parts) != count {
		cleanFail("invalid_color_profile")
	}
	for _, p := range parts {
		if p.count != count {
			cleanFail("invalid_color_profile")
		}
	}
	ordered := append([]jpegICCPart(nil), parts...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].sequence < ordered[j].sequence })
	var profile []byte
	for i, p := range ordered {
		if p.sequence != i+1 {
			cleanFail("invalid_color_profile")
		}
		profile = append(profile, p.data...)
	}
	validateJPEGICCProfile(profile, f)
	return profile
}

func validateJPEGExtendedXMP(parts []jpegExtXMPPart) []byte {
	if len(parts) == 0 {
		return nil
	}
	for _, p := range parts {
		if p.guid != parts[0].guid || p.fullLength != parts[0].fullLength {
			cleanFail("invalid_metadata")
		}
	}
	full := parts[0].fullLength
	if full < 1 || full > jpegMaxMetadataBytes {
		cleanFail("metadata_too_large")
	}
	ordered := append([]jpegExtXMPPart(nil), parts...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].offset < ordered[j].offset })
	var expected int64
	var data []byte
	for _, p := range ordered {
		if p.offset != expected || int64(len(p.data)) > full-p.offset {
			cleanFail("invalid_metadata")
		}
		expected += int64(len(p.data))
		data = append(data, p.data...)
	}
	if expected != full {
		cleanFail("invalid_metadata")
	}
	jpegValidateUTF8(data)
	return data
}

func parseJPEG(b []byte, depth int, work *int64) jpegParseResult {
	if len(b) < 4 || b[0] != 0xff || b[1] != 0xd8 {
		cleanFail("image_format_mismatch")
	}
	parts := [][]byte{b[:2]}
	removed, rewritten := false, false
	quantization := map[int]bool{}
	huffman := map[string]*jpegHuffman{}
	scanned := map[int]bool{}
	progressive := map[string]int{}
	var coefficients jpegCoefficients
	var iccParts []jpegICCPart
	var extParts []jpegExtXMPPart
	var pending []jpegPendingScan
	jumbf := map[int]map[uint32]bool{}
	var jumbfOrder []int
	jfxxCodes := map[byte]bool{}
	var frame *jpegFrame
	offset, segments, scans, metadataBytes := 2, 0, 0, 0
	sawJFIF, sawAdobe, sawPhotoshop, sawExif, sawXMP, sawEOI := false, false, false, false, false, false
	adobeTransform := -1
	var photoshopICC []byte
	restartInterval := 0

	addMetadata := func(n int) {
		metadataBytes += n
		if metadataBytes > jpegMaxMetadataBytes {
			cleanFail("metadata_too_large")
		}
	}

	for offset < len(b) {
		segments++
		if segments > jpegMaxSegments || b[offset] != 0xff {
			cleanFail("invalid_jpeg_structure")
		}
		markerStart := offset
		for offset < len(b) && b[offset] == 0xff {
			offset++
		}
		if offset >= len(b) {
			cleanFail("invalid_jpeg_structure")
		}
		marker := b[offset]
		offset++
		if marker == 0x00 || marker == 0xd8 {
			cleanFail("invalid_jpeg_structure")
		}
		if marker == 0xd9 {
			if frame == nil || scans == 0 || offset != len(b) {
				cleanFail("invalid_jpeg_structure")
			}
			parts = append(parts, b[markerStart:offset])
			sawEOI = true
			break
		}
		if marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7) {
			cleanFail("invalid_jpeg_structure")
		}
		if len(b)-offset < 2 {
			cleanFail("invalid_jpeg_structure")
		}
		length := int(b[offset])<<8 | int(b[offset+1])
		if length < 2 || length > len(b)-offset {
			cleanFail("invalid_jpeg_structure")
		}
		segmentEnd := offset + length
		payload := b[offset+2 : segmentEnd]
		raw := b[markerStart:segmentEnd]
		replacement := raw
		keep := true
		var scan *jpegScan

		switch {
		case marker == 0xc0 || marker == 0xc1 || marker == 0xc2:
			candidate := parseJPEGFrame(int(marker), payload)
			if frame != nil {
				cleanFail("duplicate_jpeg_segment")
			}
			frame = candidate
			if frame.marker == 0xc2 {
				coefficients = newProgressiveCoefficients(frame)
			}
		case marker == 0xdb:
			validateDQT(payload, quantization)
		case marker == 0xc4:
			validateDHT(payload, huffman)
		case marker == 0xdd:
			if len(payload) != 2 {
				cleanFail("invalid_jpeg_structure")
			}
			restartInterval = int(binary.BigEndian.Uint16(payload))
		case marker == 0xda:
			scans++
			if frame == nil || scans > jpegMaxScans {
				cleanFail("invalid_jpeg_structure")
			}
			for _, q := range frame.quantizationTables {
				if !quantization[q] {
					cleanFail("invalid_jpeg_structure")
				}
			}
			scan = validateSOS(payload, frame, huffman, scanned, progressive)
		case marker == 0xfe:
			keep = false
			removed = true
		case marker == 0xe0:
			addMetadata(len(payload))
			switch {
			case bytes.HasPrefix(payload, jpegJFIFPrefix):
				if sawJFIF || segments != 1 {
					cleanFail("duplicate_jpeg_segment")
				}
				clean := cleanJFIF(payload)
				if !bytes.Equal(clean, payload) {
					replacement = makeJPEGSegment(0xe0, clean)
					rewritten = true
				}
				sawJFIF = true
			case bytes.HasPrefix(payload, jpegJFXXPrefix):
				if !sawJFIF || jfxxCodes[byteAtOrZero(payload, 5)] {
					cleanFail("duplicate_jpeg_segment")
				}
				validateJFXX(payload, depth, work)
				jfxxCodes[payload[5]] = true
				keep = false
				removed = true
			default:
				cleanFail("unsupported_jpeg_app")
			}
		case marker == 0xe1:
			addMetadata(len(payload))
			switch {
			case bytes.HasPrefix(payload, jpegExifPrefix):
				if sawExif {
					cleanFail("duplicate_jpeg_segment")
				}
				sawExif = true
				clean, _ := cleanEXIF(payload[len(jpegExifPrefix):], jpegMaxMetadataBytes, true)
				if clean == nil {
					keep = false
					removed = true
				} else {
					full := append(append([]byte(nil), jpegExifPrefix...), clean...)
					if !bytes.Equal(full, payload) {
						replacement = makeJPEGSegment(0xe1, full)
						rewritten = true
					}
				}
			case bytes.HasPrefix(payload, jpegXMPPrefix):
				if sawXMP {
					cleanFail("duplicate_jpeg_segment")
				}
				sawXMP = true
				validateRemovableXMP(payload[len(jpegXMPPrefix):])
				keep = false
				removed = true
			case bytes.HasPrefix(payload, jpegXMPExtendedPrefix):
				header := len(jpegXMPExtendedPrefix)
				if len(payload) < header+40 {
					cleanFail("invalid_metadata")
				}
				guid := string(payload[header : header+32])
				if !jpegGUIDRe.MatchString(guid) {
					cleanFail("invalid_metadata")
				}
				fullLength := int64(binary.BigEndian.Uint32(payload[header+32:]))
				partOffset := int64(binary.BigEndian.Uint32(payload[header+36:]))
				data := payload[header+40:]
				if len(data) == 0 || fullLength > jpegMaxMetadataBytes || partOffset > fullLength || int64(len(data)) > fullLength-partOffset {
					cleanFail("invalid_metadata")
				}
				extParts = append(extParts, jpegExtXMPPart{guid: guid, fullLength: fullLength, offset: partOffset, data: data})
				keep = false
				removed = true
			default:
				cleanFail("unsupported_jpeg_app")
			}
		case marker == 0xe2:
			metadataBytes += len(payload)
			if metadataBytes > jpegMaxMetadataBytes || !bytes.HasPrefix(payload, jpegICCPrefix) || len(payload) <= len(jpegICCPrefix)+2 {
				cleanFail("unsupported_jpeg_app")
			}
			sequence, count := int(payload[len(jpegICCPrefix)]), int(payload[len(jpegICCPrefix)+1])
			if sequence < 1 || count < 1 || sequence > count {
				cleanFail("invalid_color_profile")
			}
			for _, p := range iccParts {
				if p.sequence == sequence {
					cleanFail("invalid_color_profile")
				}
			}
			iccParts = append(iccParts, jpegICCPart{sequence: sequence, count: count, data: payload[len(jpegICCPrefix)+2:]})
		case marker == 0xeb:
			if len(payload) < 9 || payload[0] != 0x4a || payload[1] != 0x50 {
				cleanFail("unsupported_jpeg_app")
			}
			instance := int(binary.BigEndian.Uint16(payload[2:]))
			sequence := binary.BigEndian.Uint32(payload[4:])
			if instance == 0 || sequence == 0 || jumbf[instance][sequence] {
				cleanFail("invalid_metadata")
			}
			if jumbf[instance] == nil {
				jumbf[instance] = map[uint32]bool{}
				jumbfOrder = append(jumbfOrder, instance)
			}
			jumbf[instance][sequence] = true
			keep = false
			removed = true
		case marker == 0xed:
			metadataBytes += len(payload)
			if metadataBytes > jpegMaxMetadataBytes || sawPhotoshop {
				if sawPhotoshop {
					cleanFail("duplicate_jpeg_segment")
				}
				cleanFail("metadata_too_large")
			}
			sawPhotoshop = true
			data, icc := cleanPhotoshop(payload)
			photoshopICC = icc
			if data == nil {
				keep = false
				removed = true
			} else if !bytes.Equal(data, payload) {
				replacement = makeJPEGSegment(0xed, data)
				rewritten = true
			}
		case marker == 0xee:
			addMetadata(len(payload))
			if sawAdobe {
				cleanFail("duplicate_jpeg_segment")
			}
			validateAdobe(payload)
			sawAdobe = true
			adobeTransform = int(payload[11])
		case marker == 0xc4 || marker == 0xdb || marker == 0xdd:
		default:
			if marker >= 0xe0 && marker <= 0xef {
				cleanFail("unsupported_jpeg_app")
			}
			cleanFail("invalid_jpeg_structure")
		}

		if keep {
			parts = append(parts, replacement)
		}
		offset = segmentEnd
		if marker == 0xda {
			entropyStart := offset
			expectedRestart := 0
			sawEntropy := false
			for offset < len(b) {
				if b[offset] != 0xff {
					sawEntropy = true
					offset++
					continue
				}
				codeOffset := offset + 1
				for codeOffset < len(b) && b[codeOffset] == 0xff {
					codeOffset++
				}
				if codeOffset >= len(b) {
					cleanFail("invalid_jpeg_structure")
				}
				code := b[codeOffset]
				if code == 0x00 {
					if codeOffset != offset+1 {
						cleanFail("invalid_jpeg_structure")
					}
					sawEntropy = true
					offset = codeOffset + 1
					continue
				}
				if code >= 0xd0 && code <= 0xd7 {
					if restartInterval == 0 || int(code) != 0xd0+expectedRestart {
						cleanFail("invalid_jpeg_structure")
					}
					expectedRestart = (expectedRestart + 1) % 8
					offset = codeOffset + 1
					continue
				}
				if code == 0x01 {
					offset = codeOffset + 1
					continue
				}
				break
			}
			if offset == entropyStart || !sawEntropy {
				cleanFail("invalid_jpeg_structure")
			}
			entropy := b[entropyStart:offset]
			parts = append(parts, entropy)
			snapshot := make(map[string]*jpegHuffman, len(huffman))
			for k, v := range huffman {
				snapshot[k] = v
			}
			pending = append(pending, jpegPendingScan{raw: entropy, scan: scan, huffman: snapshot, restartInterval: restartInterval})
		}
	}

	if !sawEOI || frame == nil {
		cleanFail("invalid_jpeg_structure")
	}
	for _, id := range frame.componentIDs {
		if frame.marker == 0xc2 {
			if _, ok := progressive[progKey(id, 0)]; !ok {
				cleanFail("invalid_jpeg_structure")
			}
		} else if !scanned[id] {
			cleanFail("invalid_jpeg_structure")
		}
	}
	chargeJPEGEntropyWork(work, frame, pending)
	for _, p := range pending {
		validateJPEGEntropyScan(p.raw, frame, p.scan, p.huffman, p.restartInterval, coefficients)
	}
	app2ICC := validateJPEGICC(iccParts, frame)
	if photoshopICC != nil {
		validateJPEGICCProfile(photoshopICC, frame)
	}
	if app2ICC != nil && photoshopICC != nil {
		cleanFail("invalid_color_profile")
	}
	if (adobeTransform == 1 && len(frame.componentIDs) != 3) || (adobeTransform == 2 && len(frame.componentIDs) != 4) {
		cleanFail("invalid_metadata")
	}
	if ext := validateJPEGExtendedXMP(extParts); ext != nil {
		validateRemovableXMP(ext)
	}
	for _, instance := range jumbfOrder {
		var sequences []uint32
		for s := range jumbf[instance] {
			sequences = append(sequences, s)
		}
		sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
		for i, s := range sequences {
			if s != uint32(i+1) {
				cleanFail("invalid_metadata")
			}
		}
	}
	changed := removed || rewritten
	out := b
	if changed {
		out = bytes.Join(parts, nil)
	}
	return jpegParseResult{bytes: out, changed: changed, width: frame.width, height: frame.height}
}

func byteAtOrZero(b []byte, i int) byte {
	if i < len(b) {
		return b[i]
	}
	return 0
}

// sanitizeGeneratedJPEG mirrors sanitizeGeneratedJpegMetadata(bytes, "jpeg").
func sanitizeGeneratedJPEG(b []byte) *ImageCleanResult {
	if len(b) == 0 {
		cleanFail("invalid_jpeg_structure")
	}
	if len(b) > imageCleanMaxBytes {
		cleanFail("image_too_large")
	}
	work := int64(jpegMaxEntropyWork)
	parsed := parseJPEG(b, 0, &work)
	if !isSafeGeneratedImageDimensions(int64(parsed.width), int64(parsed.height)) {
		cleanFail("invalid_dimensions")
	}
	sum := imageCleanDigest(parsed.bytes)
	source := sum
	if parsed.changed {
		source = imageCleanDigest(b)
	}
	return &ImageCleanResult{Bytes: parsed.bytes, MIMEType: "image/jpeg", Width: parsed.width, Height: parsed.height,
		SHA256: sum, SourceSHA256: source, Changed: parsed.changed}
}
