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
     * @param data - Raw bytes of the content.
     * @param record - The associated {@link ContentRecord}.
     */
    render(data: ArrayBuffer, record: ContentRecord): JSX.Element;
}
