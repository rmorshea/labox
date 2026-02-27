import { render } from 'preact';
import { fetchContentRecord, fetchContentData } from './api';
import { findRenderer, DEFAULT_RENDERER_MAP, buildRendererMap } from './renderers';
import type { Renderer } from './renderers/types';

export type { Renderer };

/**
 * Fetch the {@link ContentRecord} for `contentId`, download its raw data,
 * select the appropriate renderer by `content_type`, and mount a Preact tree
 * into `container`.
 *
 * A `labox-loading` placeholder is shown immediately while data is in-flight.
 * On error a `labox-error` element is rendered with the error message.
 *
 * @param contentId - UUID of the content record to render.
 * @param baseUrl - Base URL of the labox server (no trailing slash).
 * @param container - DOM element to mount the rendered output into.
 * @param rendererMap - Pre-built renderer map. Defaults to {@link DEFAULT_RENDERER_MAP}.
 *   Build a custom map once with {@link buildRendererMap} and reuse it across calls.
 */
export async function renderContent(
    contentId: string,
    baseUrl: string,
    container: Element,
    rendererMap: Map<string, Renderer> = DEFAULT_RENDERER_MAP,
): Promise<void> {
    render(<div class="labox-loading" />, container);

    try {
        const [record, data] = await Promise.all([
            fetchContentRecord(contentId, baseUrl),
            fetchContentData(contentId, baseUrl),
        ]);

        const renderer = findRenderer(record.content_type, rendererMap);
        render(renderer.render(data, record), container);
    } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        render(<div class="labox-error">{message}</div>, container);
    }
}
