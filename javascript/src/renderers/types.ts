import type { JSX } from 'preact';
import type { ContentRecord } from '../types';

/**
 * A renderer for a specific `content_type`.
 *
 * Renderers are plain objects — no classes or inheritance required.
 * To register a custom renderer, pass it as the first element of the
 * `renderers` array in {@link renderContent} (custom renderers take priority).
 */
export interface Renderer {
    /**
     * The content types this renderer handles.
     *
     * Use `type/*` as a wildcard to match any subtype (e.g. `image/*`).
     * {@link fallbackRenderer} uses an empty array and is always the last resort.
     */
    types: readonly string[];

    /**
     * Produce a Preact virtual-DOM tree for the given raw data.
     *
     * The stream is fresh for every call and may be consumed at any pace —
     * read it in full with {@link readAll} or process it chunk-by-chunk for
     * progressive rendering.
     *
     * @param data - Readable byte stream of the content body.
     * @param record - The associated {@link ContentRecord}.
     */
    render(data: ReadableStream<Uint8Array>, record: ContentRecord): JSX.Element;
}
