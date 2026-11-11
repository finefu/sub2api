// Recording wrapper for lib/generated-image-metadata.ts (see hooks.mjs).
import { recordCall } from "./recorder.mjs";

const orig = await import(`${process.env.GOLDEN_ORIG_PNG}?golden-orig`);
export const GENERATED_IMAGE_METADATA_PROFILE = orig.GENERATED_IMAGE_METADATA_PROFILE;
export const GeneratedImageMetadataError = orig.GeneratedImageMetadataError;
export const inspectCleanGeneratedImage = orig.inspectCleanGeneratedImage;
export function sanitizeGeneratedImageMetadata(bytes, declaredFormat, options = {}) {
  return recordCall("png", orig.sanitizeGeneratedImageMetadata, bytes, declaredFormat, options);
}
