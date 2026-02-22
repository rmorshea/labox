import { jsonRenderer } from './json';
import { csvRenderer } from './csv';
import { imageRenderer } from './image';
import { plotlyRenderer } from './plotly';
import { fallbackRenderer } from './fallback';
import type { Renderer } from './types';

export type { Renderer };
export { fallbackRenderer };

/**
 * Default set of renderers used to build the content-type lookup map.
 * {@link fallbackRenderer} is always the final fallback and need not be
 * included here.
 */
export const RENDERERS: readonly Renderer[] = [
  jsonRenderer,
  csvRenderer,
  imageRenderer,
  plotlyRenderer,
];

/**
 * Build a content-type to renderer map from a list of renderers.
 * Call this once and reuse the result with {@link findRenderer}.
 */
export function buildRendererMap(renderers: readonly Renderer[]): Map<string, Renderer> {
  const map = new Map<string, Renderer>();
  for (const r of renderers) {
    for (const t of r.types) {
      map.set(t, r);
    }
  }
  return map;
}

/** Pre-built map for the default {@link RENDERERS}. */
export const RENDERER_MAP: Map<string, Renderer> = buildRendererMap(RENDERERS);

/**
 * Find a renderer for `contentType` using a two-step heuristic:
 *
 * 1. **Exact match** — `application/vnd.plotly.v1+json`
 * 2. **Structured-syntax suffix** — `application/vnd.plotly.v1+json` → `application/json`
 * 3. {@link fallbackRenderer}
 */
export function findRenderer(
  contentType: string,
  map: Map<string, Renderer> = RENDERER_MAP,
): Renderer {
  // 1. Exact match
  const exact = map.get(contentType);
  if (exact) return exact;

  // 2. Structured-syntax suffix: type/subtype+suffix → type/suffix
  const plusIdx = contentType.indexOf('+');
  if (plusIdx !== -1) {
    const slashIdx = contentType.indexOf('/');
    const type = contentType.slice(0, slashIdx);
    const suffix = contentType.slice(plusIdx + 1);
    const suffixMatch = map.get(`${type}/${suffix}`);
    if (suffixMatch) return suffixMatch;
  }

  return fallbackRenderer;
}
