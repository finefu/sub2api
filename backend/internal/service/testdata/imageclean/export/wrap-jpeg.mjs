// Recording wrapper for lib/generated-image-jpeg-metadata.ts (see hooks.mjs).
import { recordCall } from "./recorder.mjs";

const orig = await import(`${process.env.GOLDEN_ORIG_JPEG}?golden-orig`);
export const GENERATED_JPEG_METADATA_PROFILE = orig.GENERATED_JPEG_METADATA_PROFILE;
export const GeneratedJpegMetadataError = orig.GeneratedJpegMetadataError;
export const inspectCleanGeneratedJpeg = orig.inspectCleanGeneratedJpeg;
export function sanitizeGeneratedJpegMetadata(bytes, declaredFormat, options = {}) {
  return recordCall("jpeg", orig.sanitizeGeneratedJpegMetadata, bytes, declaredFormat, options);
}
