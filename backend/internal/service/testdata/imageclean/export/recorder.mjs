// Golden-sample recorder for YeaGrow's generated-image metadata cleaners.
// Wraps sanitizeGeneratedImageMetadata / sanitizeGeneratedJpegMetadata, calls
// the real YeaGrow function, records input, output and error code, then
// returns/throws exactly what YeaGrow returned/threw.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

const OUT = process.env.GOLDEN_OUT;
if (!OUT) throw new Error("GOLDEN_OUT is not set");
mkdirSync(OUT, { recursive: true });

const records = new Map();
const sha = (bytes) => createHash("sha256").update(bytes).digest("hex");

function writeBlob(bytes) {
  const digest = sha(bytes);
  const file = `${digest}.bin.gz`;
  const path = join(OUT, file);
  if (!existsSync(path)) writeFileSync(path, gzipSync(bytes, { level: 9 }));
  return file;
}

// First stack frame in a test file identifies where the call came from.
function callSite() {
  const stack = new Error().stack?.split("\n") ?? [];
  for (const line of stack) {
    const match = /(tests\/[^\s:()]+\.test\.m?ts):(\d+)/.exec(line) || /(extra-cases\.test\.mts):(\d+)/.exec(line);
    if (match) return `${match[1]}:${match[2]}`;
  }
  return "unknown";
}

function label() {
  return globalThis.__goldenLabel ?? callSite();
}

export async function recordCall(format, original, bytes, declaredFormat, options) {
  const recordable = bytes instanceof Uint8Array && declaredFormat === format;
  if (!recordable) return original(bytes, declaredFormat, options);
  const input = Uint8Array.from(bytes);
  const external = options?.externalProvenancePresent === true;
  const key = sha(Buffer.concat([Buffer.from(`${format}\0${external}\0`), input]));
  const source = label();
  let result;
  let error;
  try {
    result = await original(bytes, declaredFormat, options);
  } catch (caught) {
    error = caught;
  }
  if (!records.has(key)) {
    const entry = {
      id: key.slice(0, 16),
      format,
      external_provenance: external,
      source,
      input: writeBlob(input),
      input_sha256: sha(input),
      input_size: input.byteLength,
    };
    if (error) {
      entry.result = { ok: false, code: typeof error?.code === "string" ? error.code : `!${error?.name ?? "Error"}` };
    } else {
      const out = Uint8Array.from(result.bytes);
      let idempotent = false;
      try {
        const again = await original(out, declaredFormat, {});
        idempotent = Buffer.compare(Buffer.from(again.bytes), Buffer.from(out)) === 0;
      } catch {
        idempotent = false;
      }
      entry.result = {
        ok: true,
        output: Buffer.compare(Buffer.from(out), Buffer.from(input)) === 0 ? null : writeBlob(out),
        sha256: result.info.sha256,
        source_sha256: result.sourceSha256,
        mime_type: result.info.mimeType,
        width: result.info.width,
        height: result.info.height,
        byte_size: result.info.byteSize,
        idempotent,
      };
    }
    records.set(key, entry);
  } else {
    records.get(key).also = [...new Set([...(records.get(key).also ?? []), source])].slice(0, 8);
  }
  if (error) throw error;
  return result;
}

process.on("exit", () => {
  const entries = [...records.values()].sort((a, b) => a.format.localeCompare(b.format) || a.source.localeCompare(b.source, "en", { numeric: true }) || a.id.localeCompare(b.id));
  writeFileSync(join(OUT, "manifest.json"), JSON.stringify({
    yeagrow_sha: process.env.GOLDEN_YEAGROW_SHA ?? "unknown",
    generated_by: "testdata/imageclean/export/export.sh",
    count: entries.length,
    samples: entries,
  }, null, 1) + "\n");
});
