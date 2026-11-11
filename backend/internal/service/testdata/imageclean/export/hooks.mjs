// Node module hooks: every import of YeaGrow's two cleaner modules (from the
// YeaGrow tests or the extra cases) is redirected to a recording wrapper. The
// wrapper itself imports the real module with a "?golden-orig" query.
import { registerHooks } from "node:module";

const here = new URL(".", import.meta.url);
const targets = new Map([
  ["/lib/generated-image-metadata.ts", new URL("wrap-png.mjs", here).href],
  ["/lib/generated-image-jpeg-metadata.ts", new URL("wrap-jpeg.mjs", here).href],
]);

registerHooks({
  resolve(specifier, context, nextResolve) {
    const resolved = nextResolve(specifier, context);
    if (resolved.url.includes("?golden-orig")) return resolved;
    for (const [suffix, wrapper] of targets) {
      if (resolved.url.endsWith(suffix)) {
        process.env[`GOLDEN_ORIG_${suffix.includes("jpeg") ? "JPEG" : "PNG"}`] = resolved.url;
        return { url: wrapper, shortCircuit: true, format: "module" };
      }
    }
    return resolved;
  },
});
