package service

// Strict RFC 1950/1951 inflate with the exact acceptance rules of YeaGrow's
// inflate() in lib/generated-image-metadata.ts, which drives pako 2.1.0 in raw
// mode with dmax = the zlib header's advertised window:
//   - CMF/FLG: CM 8, CINFO <= 7, FCHECK, no FDICT; at least 8 bytes total;
//   - zlib's data errors (block type, stored lengths, code-length/literal/
//     distance code sets per inflate_table, invalid codes, too-far distances
//     including dist > 1<<windowBits);
//   - the DEFLATE stream must end in the last byte before the Adler-32 trailer;
//   - Adler-32 over the output must match;
//   - output is delivered in 16 KiB pieces (pako chunkSize) and once at stream
//     end; the limit is checked on delivery, and output pending when a data
//     error occurs is never delivered (pako returns before onData).
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import "encoding/binary"

const cleanInflateChunk = 16 * 1024

var (
	cleanLenBase   = [29]int{3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258}
	cleanLenExtra  = [29]uint{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0}
	cleanDistBase  = [30]int{1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577}
	cleanDistExtra = [30]uint{0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13}
	cleanCodeOrder = [19]int{16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15}
)

// cleanInflateStop aborts decoding with "data error or truncated input".
type cleanInflateStop struct{}

type cleanHuffman struct {
	count  [16]int
	symbol []int
	maxLen uint
	table  []uint32 // index: next maxLen bits (LSB-first); value: sym<<4 | len, 0 = invalid
}

const (
	cleanHuffCodes = iota
	cleanHuffLens
	cleanHuffDists
)

// newCleanHuffman applies zlib inflate_table's acceptance rules.
func newCleanHuffman(lengths []uint8, kind int) (*cleanHuffman, bool) {
	h := &cleanHuffman{}
	for _, l := range lengths {
		h.count[l]++
	}
	maxLen := 0
	for l := 15; l >= 1; l-- {
		if h.count[l] != 0 {
			maxLen = l
			break
		}
	}
	if maxLen == 0 {
		// zlib accepts an empty set and builds an all-invalid table. For code
		// lengths that always ends in an error; for distances it is valid as
		// long as no distance is ever decoded.
		if kind == cleanHuffCodes {
			return nil, false
		}
		h.count[0] = 0
		return h, true
	}
	left := 1
	for l := 1; l <= 15; l++ {
		left <<= 1
		left -= h.count[l]
		if left < 0 {
			return nil, false // over-subscribed
		}
	}
	if left > 0 && (kind == cleanHuffCodes || maxLen != 1) {
		return nil, false // incomplete
	}
	h.count[0] = 0
	offs := [16]int{}
	for l := 1; l < 15; l++ {
		offs[l+1] = offs[l] + h.count[l]
	}
	h.symbol = make([]int, offs[15]+h.count[15])
	for sym, l := range lengths {
		if l != 0 {
			h.symbol[offs[l]] = sym
			offs[l]++
		}
	}
	h.maxLen = uint(maxLen)
	h.table = make([]uint32, 1<<maxLen)
	code, idx := 0, 0
	for l := 1; l <= maxLen; l++ {
		for i := 0; i < h.count[l]; i++ {
			sym := h.symbol[idx]
			idx++
			rev := 0
			for b := 0; b < l; b++ {
				rev |= ((code >> b) & 1) << (l - 1 - b)
			}
			for j := rev; j < len(h.table); j += 1 << l {
				h.table[j] = uint32(sym)<<4 | uint32(l)
			}
			code++
		}
		code <<= 1
	}
	return h, true
}

type cleanInflater struct {
	in     []byte
	bitPos int
	nbits  int

	window  [32768]byte
	outPos  int // total bytes produced
	pending []byte
	dmax    int

	limit     int
	total     int
	adlerLow  uint32
	adlerHigh uint32
	onData    func([]byte)
	limitCode string
}

func (d *cleanInflater) stop() { panic(cleanInflateStop{}) }

func (d *cleanInflater) bits(n int) int {
	if d.bitPos+n > d.nbits {
		d.stop() // zlib waits for more input; the stream never ends
	}
	v := 0
	for i := 0; i < n; i++ {
		p := d.bitPos + i
		v |= int(d.in[p>>3]>>(p&7)&1) << i
	}
	d.bitPos += n
	return v
}

func (d *cleanInflater) decode(h *cleanHuffman) int {
	if h.maxLen == 0 {
		d.stop() // empty distance set: any use is an invalid code
	}
	if d.bitPos+int(h.maxLen) <= d.nbits {
		v := 0
		for i := 0; i < int(h.maxLen); i++ {
			p := d.bitPos + i
			v |= int(d.in[p>>3]>>(p&7)&1) << i
		}
		e := h.table[v]
		if e == 0 {
			d.stop()
		}
		d.bitPos += int(e & 15)
		return int(e >> 4)
	}
	// Near the end of input: canonical bit-by-bit decode (puff), which, like
	// zlib, decodes a code as soon as its own bits are available.
	code, first, index := 0, 0, 0
	for l := 1; l <= 15; l++ {
		code |= d.bits(1)
		count := h.count[l]
		if code-count < first {
			return h.symbol[index+(code-first)]
		}
		index += count
		first += count
		first <<= 1
		code <<= 1
	}
	d.stop()
	return 0
}

func (d *cleanInflater) emit(b byte) {
	d.window[d.outPos&32767] = b
	d.outPos++
	d.pending = append(d.pending, b)
	if len(d.pending) == cleanInflateChunk {
		d.deliver()
	}
}

func (d *cleanInflater) deliver() {
	if len(d.pending) == 0 {
		return
	}
	if len(d.pending) > d.limit-d.total {
		cleanFail(d.limitCode)
	}
	d.total += len(d.pending)
	for off := 0; off < len(d.pending); {
		end := off + 5552
		if end > len(d.pending) {
			end = len(d.pending)
		}
		for ; off < end; off++ {
			d.adlerLow += uint32(d.pending[off])
			d.adlerHigh += d.adlerLow
		}
		d.adlerLow %= 65521
		d.adlerHigh %= 65521
	}
	d.onData(d.pending)
	d.pending = d.pending[:0]
}

func (d *cleanInflater) codes(lit, dist *cleanHuffman) {
	for {
		sym := d.decode(lit)
		if sym < 256 {
			d.emit(byte(sym))
			continue
		}
		if sym == 256 {
			return
		}
		sym -= 257
		if sym >= 29 {
			d.stop() // invalid literal/length code (286, 287)
		}
		length := cleanLenBase[sym] + d.bits(int(cleanLenExtra[sym]))
		ds := d.decode(dist)
		if ds >= 30 {
			d.stop() // invalid distance code (30, 31)
		}
		distance := cleanDistBase[ds] + d.bits(int(cleanDistExtra[ds]))
		if distance > d.dmax || distance > d.outPos {
			d.stop() // invalid distance too far back
		}
		for i := 0; i < length; i++ {
			d.emit(d.window[(d.outPos-distance)&32767])
		}
	}
}

var (
	cleanFixedLit  *cleanHuffman
	cleanFixedDist *cleanHuffman
)

func init() {
	lens := make([]uint8, 288)
	for i := range lens {
		switch {
		case i < 144:
			lens[i] = 8
		case i < 256:
			lens[i] = 9
		case i < 280:
			lens[i] = 7
		default:
			lens[i] = 8
		}
	}
	cleanFixedLit, _ = newCleanHuffman(lens, cleanHuffLens)
	dl := make([]uint8, 32)
	for i := range dl {
		dl[i] = 5
	}
	cleanFixedDist, _ = newCleanHuffman(dl, cleanHuffDists)
}

func (d *cleanInflater) dynamic() {
	nlen := d.bits(5) + 257
	ndist := d.bits(5) + 1
	ncode := d.bits(4) + 4
	if nlen > 286 || ndist > 30 {
		d.stop()
	}
	var cl [19]uint8
	for i := 0; i < ncode; i++ {
		cl[cleanCodeOrder[i]] = uint8(d.bits(3))
	}
	codes, ok := newCleanHuffman(cl[:], cleanHuffCodes)
	if !ok {
		d.stop()
	}
	lengths := make([]uint8, nlen+ndist)
	for have := 0; have < nlen+ndist; {
		sym := d.decode(codes)
		if sym < 16 {
			lengths[have] = uint8(sym)
			have++
			continue
		}
		var value uint8
		var copies int
		switch sym {
		case 16:
			if have == 0 {
				d.stop()
			}
			value = lengths[have-1]
			copies = 3 + d.bits(2)
		case 17:
			copies = 3 + d.bits(3)
		default:
			copies = 11 + d.bits(7)
		}
		if have+copies > nlen+ndist {
			d.stop()
		}
		for ; copies > 0; copies-- {
			lengths[have] = value
			have++
		}
	}
	if lengths[256] == 0 {
		d.stop() // missing end-of-block
	}
	lit, ok := newCleanHuffman(lengths[:nlen], cleanHuffLens)
	if !ok {
		d.stop()
	}
	dist, ok := newCleanHuffman(lengths[nlen:], cleanHuffDists)
	if !ok {
		d.stop()
	}
	d.codes(lit, dist)
}

func (d *cleanInflater) stored() {
	d.bitPos = (d.bitPos + 7) &^ 7
	if d.bitPos+32 > d.nbits {
		d.stop()
	}
	at := d.bitPos >> 3
	n := int(d.in[at]) | int(d.in[at+1])<<8
	if n != (int(d.in[at+2])|int(d.in[at+3])<<8)^0xffff {
		d.stop() // invalid stored block lengths
	}
	d.bitPos += 32
	if d.bitPos+n*8 > d.nbits {
		d.stop()
	}
	start := d.bitPos >> 3
	for i := 0; i < n; i++ {
		d.emit(d.in[start+i])
	}
	d.bitPos += n * 8
}

// cleanInflate returns the total output length; it fails with code for any
// stream error and limitCode when delivered output exceeds limit.
func cleanInflate(data []byte, limit int, onData func([]byte), code, limitCode string) int {
	if len(data) < 8 {
		cleanFail(code)
	}
	cmf, flg := int(data[0]), int(data[1])
	if cmf&15 != 8 || cmf>>4 > 7 || (cmf<<8|flg)%31 != 0 || flg&0x20 != 0 {
		cleanFail(code)
	}
	raw := data[2 : len(data)-4]
	expectedAdler := binary.BigEndian.Uint32(data[len(data)-4:])
	d := &cleanInflater{
		in: raw, nbits: len(raw) * 8, dmax: 1 << (cmf>>4 + 8),
		limit: limit, adlerLow: 1, onData: onData, limitCode: limitCode,
		pending: make([]byte, 0, cleanInflateChunk),
	}
	if !d.run() {
		cleanFail(code)
	}
	// Stream end: pako delivers the remaining output before YeaGrow checks
	// that the end marker sits in the last raw byte.
	d.deliver()
	if (d.bitPos+7)>>3 != len(raw) || (d.adlerHigh<<16|d.adlerLow) != expectedAdler {
		cleanFail(code)
	}
	return d.total
}

// run decodes until the final block; false on any data error or truncation.
func (d *cleanInflater) run() (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, stop := r.(cleanInflateStop); stop {
				ok = false
				return
			}
			panic(r)
		}
	}()
	for {
		final := d.bits(1)
		switch d.bits(2) {
		case 0:
			d.stored()
		case 1:
			d.codes(cleanFixedLit, cleanFixedDist)
		case 2:
			d.dynamic()
		default:
			d.stop() // invalid block type
		}
		if final == 1 {
			return true
		}
	}
}
