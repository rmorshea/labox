import type { ContentRecord } from './types';

/**
 * Fetch a {@link ContentRecord} from the labox server.
 *
 * @param contentId - UUID of the content record.
 * @param baseUrl - Base URL of the labox server (no trailing slash).
 */
export async function fetchContentRecord(
    contentId: string,
    baseUrl: string,
): Promise<ContentRecord> {
    const res = await fetch(`${baseUrl}/contents/${contentId}`);
    if (!res.ok) {
        throw new Error(`Failed to fetch content record: ${res.status} ${res.statusText}`);
    }
    return res.json() as Promise<ContentRecord>;
}

/**
 * Fetch the raw data stream for a content record.
 *
 * Returns the response body as a {@link ReadableStream} so that renderers
 * can either buffer the whole payload or consume it progressively.
 *
 * @param contentId - UUID of the content record.
 * @param baseUrl - Base URL of the labox server (no trailing slash).
 */
export async function fetchContentData(
    contentId: string,
    baseUrl: string,
): Promise<ReadableStream<Uint8Array>> {
    const res = await fetch(`${baseUrl}/contents/${contentId}/data`);
    if (!res.ok) {
        throw new Error(`Failed to fetch content data: ${res.status} ${res.statusText}`);
    }
    if (!res.body) {
        throw new Error('Response body is null');
    }
    return res.body;
}
