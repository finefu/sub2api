// Extra golden-sample inputs on top of YeaGrow's own cleaner tests: realistic
// encoder output (seedgen), combined metadata, truncation/corruption,
// dimension and 10 MiB limits. Only YeaGrow's real cleaners decide outcomes;
// this file asserts nothing about them, it just feeds inputs to the recorder.
import test from "node:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { deflateSync } from "node:zlib";

const Y = process.env.YEAGROW_DIR!;
const SEEDS = process.env.SEEDS_DIR!;
const { sanitizeGeneratedImageMetadata } = await import(`${Y}/lib/generated-image-metadata.ts`);
const { sanitizeGeneratedJpegMetadata } = await import(`${Y}/lib/generated-image-jpeg-metadata.ts`);
const { insertPngChunks, joinPngParts, pngChunk, pngFixture, readPngChunks } = await import(`${Y}/tests/helpers/png-fixture.ts`);

const enc = (s: string) => new TextEncoder().encode(s);
const seed = (name: string) => new Uint8Array(readFileSync(join(SEEDS, name)));
const join2 = (...parts: Uint8Array[]) => joinPngParts(...parts) as Uint8Array;

async function feed(label: string, format: "png" | "jpeg", bytes: Uint8Array) {
  (globalThis as any).__goldenLabel = `extra:${label}`;
  try {
    if (format === "png") await sanitizeGeneratedImageMetadata(bytes, "png");
    else await sanitizeGeneratedJpegMetadata(bytes, "jpeg");
  } catch { /* outcome is recorded */ }
  (globalThis as any).__goldenLabel = undefined;
}

// ---------- shared metadata builders ----------
function icc(color = "RGB ", tags = 0) {
  const profile = new Uint8Array(132 + tags * 12 + tags * 8);
  const v = new DataView(profile.buffer);
  v.setUint32(0, profile.byteLength);
  profile.set(enc(color), 16); profile.set(enc("XYZ "), 20); profile.set(enc("acsp"), 36);
  v.setUint32(128, tags);
  for (let i = 0; i < tags; i++) {
    profile.set(enc(["desc", "wtpt", "rXYZ", "gXYZ"][i]), 132 + i * 12);
    v.setUint32(136 + i * 12, 132 + tags * 12 + i * 8);
    v.setUint32(140 + i * 12, 8);
  }
  return profile;
}

// EXIF with orientation plus private camera tags and a GPS IFD.
function exif(orientation: number | null, little = false) {
  const t = new Uint8Array(128);
  const v = new DataView(t.buffer);
  t.set(little ? [73, 73] : [77, 77]); v.setUint16(2, 42, little); v.setUint32(4, 8, little);
  const entries: Array<[number, number, number, number]> = [];
  if (orientation !== null) entries.push([0x0112, 3, 1, orientation << (little ? 0 : 16)]);
  entries.push([0x010f, 2, 4, little ? 0x0045_4e4f : 0x4f4e_4500]); // Make "ONE\0" inline (4 bytes)
  entries.push([0x8825, 4, 1, 64]); // GPS IFD at 64
  v.setUint16(8, entries.length, little);
  entries.forEach(([tag, type, count, value], i) => {
    const e = 10 + i * 12;
    v.setUint16(e, tag, little); v.setUint16(e + 2, type, little); v.setUint32(e + 4, count, little);
    if (type === 3 && !little) v.setUint16(e + 8, value >>> 16, little);
    else if (type === 3) v.setUint16(e + 8, value, little);
    else v.setUint32(e + 8, value, little);
  });
  v.setUint32(10 + entries.length * 12, 0, little);
  v.setUint16(64, 1, little); // GPS IFD: GPSVersionID
  v.setUint16(66, 0x0000, little); v.setUint16(68, 1, little); v.setUint32(70, 4, little); v.setUint32(74, 0x02020000, little);
  v.setUint32(78, 0, little);
  return t;
}

// ---------- PNG ----------
const pngText = (keyword: string, text: string) => pngChunk("tEXt", enc(`${keyword}\0${text}`));
const pngZText = (keyword: string, text: string) => pngChunk("zTXt", join2(enc(`${keyword}\0`), Uint8Array.of(0), deflateSync(enc(text))));
const pngIText = (keyword: string, text: string, compressed = false) => pngChunk("iTXt",
  join2(enc(`${keyword}\0`), Uint8Array.of(compressed ? 1 : 0, 0), enc("en\0"), enc("\0"), compressed ? deflateSync(enc(text)) : enc(text)));
const SOURCE_XMP = `<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:Iptc4xmpExt="http://iptc.org/std/Iptc4xmpExt/2008-02-29/"><rdf:Description rdf:about="" Iptc4xmpExt:DigitalSourceType="http://cv.iptc.org/newscodes/digitalsourcetype/trainedAlgorithmicMedia"/></rdf:RDF></x:xmpmeta>`;

function ihdrPng(width: number, height: number, depth: number, color: number, rowBytes: number) {
  const ihdr = new Uint8Array(13);
  const v = new DataView(ihdr.buffer);
  v.setUint32(0, width); v.setUint32(4, height); ihdr[8] = depth; ihdr[9] = color;
  const raw = new Uint8Array((rowBytes + 1) * height);
  return join2(Uint8Array.of(137, 80, 78, 71, 13, 10, 26, 10), pngChunk("IHDR", ihdr),
    pngChunk("IDAT", deflateSync(raw, { level: 9 })), pngChunk("IEND"));
}

function rechunk(bytes: Uint8Array, mutate: (chunks: any[]) => Uint8Array[]) {
  return join2(bytes.subarray(0, 8), ...mutate(readPngChunks(bytes)));
}

test("extra PNG cases", async () => {
  for (const name of ["seed-photo.png", "seed-rgba64.png", "seed-paletted.png", "seed-best.png"]) {
    const clean = seed(name);
    await feed(`png clean ${name}`, "png", clean);
    await feed(`png all-metadata ${name}`, "png", insertPngChunks(clean, [
      pngChunk("eXIf", exif(6)), pngText("Author", "Jane"), pngZText("Software", "Kemeitv"),
      pngIText("XML:com.adobe.xmp", SOURCE_XMP), pngChunk("caBX", enc("c2pa manifest bytes")),
      pngChunk("dSIG", enc("legacy signature")), pngChunk("tIME", Uint8Array.of(7, 234, 11, 11, 8, 0, 0)),
      pngIText("Copyright", "(c) source", true),
    ]));
    await feed(`png truncated-tail ${name}`, "png", clean.subarray(0, clean.byteLength - 7));
    await feed(`png truncated-mid ${name}`, "png", clean.subarray(0, Math.floor(clean.byteLength / 2)));
    const badCrc = clean.slice(); badCrc[40] ^= 0x01;
    await feed(`png bad-crc ${name}`, "png", badCrc);
    await feed(`png corrupt-idat ${name}`, "png", rechunk(clean, (chunks) => chunks.map((c: any) => {
      if (c.type !== "IDAT") return c.raw;
      const data = c.data.slice(); data[Math.floor(data.byteLength / 2)] ^= 0x55;
      return pngChunk("IDAT", data);
    })));
    await feed(`png split-idat ${name}`, "png", rechunk(clean, (chunks) => chunks.flatMap((c: any) => {
      if (c.type !== "IDAT") return [c.raw];
      const out: Uint8Array[] = [];
      for (let i = 0; i < c.data.byteLength; i += 997) out.push(pngChunk("IDAT", c.data.subarray(i, i + 997)));
      return out;
    })));
  }
  const base = pngFixture(8, 6);
  await feed("png icc rgb with tags", "png", insertPngChunks(base, [pngChunk("iCCP", join2(enc("sRGB\0"), Uint8Array.of(0), deflateSync(icc("RGB ", 4))))]));
  await feed("png icc gray mismatch", "png", insertPngChunks(base, [pngChunk("iCCP", join2(enc("sRGB\0"), Uint8Array.of(0), deflateSync(icc("GRAY"))))]));
  await feed("png exif little-endian orientation 8", "png", insertPngChunks(base, [pngChunk("eXIf", exif(8, true))]));
  await feed("png exif without orientation", "png", insertPngChunks(base, [pngChunk("eXIf", exif(null))]));
  await feed("png xmp private field", "png", insertPngChunks(base, [pngIText("XML:com.adobe.xmp",
    SOURCE_XMP.replace('rdf:about=""', 'rdf:about="" xmp:CreatorTool="Camera"'))]));
  await feed("png unknown text key", "png", insertPngChunks(base, [pngText("Comment", "hello")]));
  await feed("png gps text", "png", insertPngChunks(base, [pngText("GPS Latitude", "31.2")]));
  // Dimension limits (1-bit grayscale keeps pixel data tiny).
  await feed("png 4096x4096 max regular", "png", ihdrPng(4096, 4096, 1, 0, 512));
  await feed("png 4097x1 too wide", "png", ihdrPng(4097, 1, 1, 0, 513));
  await feed("png 4300x4300 rounded 4k", "png", ihdrPng(4300, 4300, 1, 0, 538));
  await feed("png 4301x4300 beyond 4k", "png", ihdrPng(4301, 4300, 1, 0, 538));
  await feed("png 0x1", "png", ihdrPng(0, 1, 1, 0, 0));
  // Size limit.
  const over = insertPngChunks(base, [pngText("Author", "a".repeat(10 * 1024 * 1024))]);
  await feed("png over 10MiB", "png", over);
  await feed("png empty", "png", new Uint8Array(0));
  await feed("png signature only", "png", base.subarray(0, 8));
});

// ---------- JPEG ----------
function segment(marker: number, payload: Uint8Array) {
  const out = new Uint8Array(payload.byteLength + 4);
  out.set([0xff, marker, (payload.byteLength + 2) >>> 8, (payload.byteLength + 2) & 0xff]);
  out.set(payload, 4);
  return out;
}
const afterSoi = (jpg: Uint8Array, ...parts: Uint8Array[]) => join2(jpg.subarray(0, 2), ...parts, jpg.subarray(2));
const jfif = (thumb = 0) => segment(0xe0, join2(enc("JFIF\0"), Uint8Array.of(1, 2, 1, 0, 72, 0, 72, thumb, thumb), new Uint8Array(thumb * thumb * 3)));
const xmp = (text: string) => segment(0xe1, join2(enc("http://ns.adobe.com/xap/1.0/\0"), enc(text)));
const extXmp = (guid: string, full: number, offset: number, data: Uint8Array) => {
  const head = new Uint8Array(8); const v = new DataView(head.buffer); v.setUint32(0, full); v.setUint32(4, offset);
  return segment(0xe1, join2(enc("http://ns.adobe.com/xmp/extension/\0"), enc(guid), head, data));
};
const iccSeg = (profile: Uint8Array, sequence: number, count: number) => segment(0xe2, join2(enc("ICC_PROFILE\0"), Uint8Array.of(sequence, count), profile));
const jumbf = (instance: number, sequence: number) => {
  const p = new Uint8Array(16); const v = new DataView(p.buffer);
  p.set([0x4a, 0x50]); v.setUint16(2, instance); v.setUint32(4, sequence); p.set(enc("jumb"), 12);
  return segment(0xeb, p);
};
const adobe = (transform: number) => segment(0xee, join2(enc("Adobe"), Uint8Array.of(0, 100, 0, 0, 0, 0, transform)));
const photoshopIptc = () => {
  const iptc = Uint8Array.of(0x1c, 2, 110, 0, 4, ...enc("Test"));
  const block = new Uint8Array(12 + iptc.byteLength + (iptc.byteLength % 2));
  block.set(enc("8BIM"), 0); block.set([0x04, 0x04, 0, 0], 4); new DataView(block.buffer).setUint32(8, iptc.byteLength); block.set(iptc, 12);
  return segment(0xed, join2(enc("Photoshop 3.0\0"), block));
};
const exifSeg = (orientation: number | null, little = false) => segment(0xe1, join2(Uint8Array.of(69, 120, 105, 102, 0, 0), exif(orientation, little)));

function patchSof(jpg: Uint8Array, width: number, height: number) {
  const out = jpg.slice();
  for (let i = 2; i < out.byteLength - 9; i++) {
    if (out[i] === 0xff && (out[i + 1] === 0xc0 || out[i + 1] === 0xc2)) {
      out[i + 5] = height >>> 8; out[i + 6] = height & 255; out[i + 7] = width >>> 8; out[i + 8] = width & 255;
      return out;
    }
  }
  throw new Error("no SOF");
}

test("extra JPEG cases", async () => {
  for (const name of ["seed-photo-420.jpg", "seed-gray.jpg"]) {
    const clean = seed(name);
    const rgb = name !== "seed-gray.jpg";
    await feed(`jpeg clean ${name}`, "jpeg", clean);
    const profile = icc(rgb ? "RGB " : "GRAY", 2);
    const half = Math.floor(profile.byteLength / 2);
    await feed(`jpeg multi-app ${name}`, "jpeg", afterSoi(clean,
      jfif(2), exifSeg(3), xmp("<x:xmpmeta><rdf:Description xmp:CreatorTool=\"Cam\"/></x:xmpmeta>"),
      iccSeg(profile.subarray(0, half), 1, 2), iccSeg(profile.subarray(half), 2, 2),
      photoshopIptc(), segment(0xfe, enc("encoder comment")), jumbf(1, 1), jumbf(1, 2),
      ...(rgb ? [adobe(1)] : []),
    ));
    await feed(`jpeg exif little-endian ${name}`, "jpeg", afterSoi(clean, exifSeg(6, true)));
    await feed(`jpeg exif no orientation ${name}`, "jpeg", afterSoi(clean, exifSeg(null)));
    await feed(`jpeg xmp with orientation ${name}`, "jpeg", afterSoi(clean, xmp("<tiff:Orientation>6</tiff:Orientation>")));
    await feed(`jpeg xmp color ${name}`, "jpeg", afterSoi(clean, xmp("<exif:ColorSpace>1</exif:ColorSpace>")));
    const ext = enc("<x:xmpmeta>extended</x:xmpmeta>");
    await feed(`jpeg extended xmp ${name}`, "jpeg", afterSoi(clean, xmp("<x:xmpmeta/>"),
      extXmp("0123456789ABCDEF0123456789ABCDEF", ext.byteLength, 0, ext.subarray(0, 10)),
      extXmp("0123456789ABCDEF0123456789ABCDEF", ext.byteLength, 10, ext.subarray(10))));
    await feed(`jpeg icc wrong colorspace ${name}`, "jpeg", afterSoi(clean, iccSeg(icc(rgb ? "GRAY" : "RGB "), 1, 1)));
    await feed(`jpeg jumbf gap ${name}`, "jpeg", afterSoi(clean, jumbf(1, 1), jumbf(1, 3)));
    await feed(`jpeg truncated-tail ${name}`, "jpeg", clean.subarray(0, clean.byteLength - 2));
    await feed(`jpeg truncated-mid ${name}`, "jpeg", clean.subarray(0, Math.floor(clean.byteLength * 0.6)));
    const corrupt = clean.slice(); corrupt[Math.floor(clean.byteLength * 0.7)] = 0xff;
    await feed(`jpeg corrupt-entropy ${name}`, "jpeg", corrupt);
    await feed(`jpeg 4097 wide ${name}`, "jpeg", patchSof(clean, 4097, 10));
    await feed(`jpeg app15 ${name}`, "jpeg", afterSoi(clean, segment(0xef, enc("private"))));
  }
  const photo = seed("seed-photo-420.jpg");
  const coms: Uint8Array[] = [];
  for (let size = photo.byteLength; size <= 10 * 1024 * 1024; size += 65_537) coms.push(segment(0xfe, new Uint8Array(65_533)));
  await feed("jpeg over 10MiB", "jpeg", afterSoi(photo, ...coms));
  await feed("jpeg empty", "jpeg", new Uint8Array(0));
  await feed("jpeg soi only", "jpeg", Uint8Array.of(0xff, 0xd8, 0xff, 0xd9));
});

// ---------- deterministic mutation pass (differential coverage) ----------
// GOLDEN_MUTATIONS mutations per seed (committed corpus: small; exploratory
// runs can use thousands). PNG mutations re-compute the chunk CRC so they reach
// the DEFLATE decoder and metadata parsers instead of stopping at the CRC check.
const MUTATIONS = Number(process.env.GOLDEN_MUTATIONS ?? "24");
function lcg(seed: number) {
  let state = seed >>> 0;
  return (n: number) => { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; return state % n; };
}
function mutateBytes(input: Uint8Array, rnd: (n: number) => number) {
  const kind = rnd(6);
  const at = rnd(Math.max(1, input.byteLength));
  const out = Array.from(input);
  if (kind === 0) out[at] ^= 1 << rnd(8);
  else if (kind === 1) out[at] = [0x00, 0xff, 0x80, 0x01][rnd(4)];
  else if (kind === 2) out.splice(at, 1);
  else if (kind === 3) out.splice(at, 0, rnd(256));
  else if (kind === 4) return input.subarray(0, at);
  else out[at] = rnd(256);
  return Uint8Array.from(out);
}

test("mutation pass", async () => {
  if (!MUTATIONS) return;
  const smallPng = seed("seed-small.png");
  const pngSeeds: Array<[string, Uint8Array]> = [
    ["small", smallPng],
    ["small-meta", insertPngChunks(smallPng, [pngChunk("eXIf", exif(6)), pngZText("Software", "Kemeitv generator"),
      pngIText("XML:com.adobe.xmp", SOURCE_XMP, true), pngChunk("iCCP", join2(enc("sRGB\0"), Uint8Array.of(0), deflateSync(icc("RGB ", 2)))),
      pngText("Author", "Jane"), pngChunk("caBX", enc("manifest"))])],
    ["fixture-interlaced", ihdrInterlaced()],
  ];
  for (const [name, source] of pngSeeds) {
    const rnd = lcg(0x5eed + name.length);
    const chunks = readPngChunks(source);
    for (let i = 0; i < MUTATIONS; i++) {
      const target = rnd(chunks.length);
      const mutated = join2(source.subarray(0, 8), ...chunks.map((c: any, index: number) =>
        index === target && c.data.byteLength ? pngChunk(c.type, mutateBytes(c.data, rnd)) : c.raw));
      await feed(`png mutation ${name} #${i}`, "png", mutated);
    }
  }
  const smallJpeg = seed("seed-small.jpg");
  const jpegSeeds: Array<[string, Uint8Array]> = [
    ["small", smallJpeg],
    ["small-meta", afterSoi(smallJpeg, jfif(1), exifSeg(8), xmp("<x:xmpmeta/>"), iccSeg(icc("RGB ", 1), 1, 1),
      segment(0xfe, enc("c")), jumbf(2, 1), adobe(1))],
  ];
  for (const [name, source] of jpegSeeds) {
    const rnd = lcg(0xbeef + name.length);
    for (let i = 0; i < MUTATIONS; i++) await feed(`jpeg mutation ${name} #${i}`, "jpeg", mutateBytes(source, rnd));
  }
});

function ihdrInterlaced() {
  // 13x7 RGB 8-bit Adam7 image with a real filtered/compressed stream.
  const width = 13, height = 7, bpp = 3;
  const passes = [[0, 0, 8, 8], [4, 0, 8, 8], [0, 4, 4, 8], [2, 0, 4, 4], [0, 2, 2, 4], [1, 0, 2, 2], [0, 1, 1, 2]];
  const rows: number[] = [];
  for (const [sx, sy, dx, dy] of passes) {
    const cols = Math.max(0, Math.ceil((width - sx) / dx));
    const rws = Math.max(0, Math.ceil((height - sy) / dy));
    if (!cols || !rws) continue;
    for (let r = 0; r < rws; r++) { rows.push(r % 5); for (let c = 0; c < cols * bpp; c++) rows.push((r * 31 + c * 7) & 255); }
  }
  const ihdr = new Uint8Array(13); const v = new DataView(ihdr.buffer);
  v.setUint32(0, width); v.setUint32(4, height); ihdr[8] = 8; ihdr[9] = 2; ihdr[12] = 1;
  return join2(Uint8Array.of(137, 80, 78, 71, 13, 10, 26, 10), pngChunk("IHDR", ihdr),
    pngChunk("IDAT", deflateSync(Uint8Array.from(rows))), pngChunk("IEND"));
}
