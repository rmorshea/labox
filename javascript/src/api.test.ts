import { describe, it, expect, afterEach, spyOn } from 'bun:test';
import { fetchContentRecord, fetchContentData } from './api';
import type { ContentRecord } from './types';

const BASE_URL = 'http://localhost:8000';
const CONTENT_ID = '00000000-0000-0000-0000-000000000001';

const RECORD: ContentRecord = {
    id: CONTENT_ID,
    manifest_id: '00000000-0000-0000-0000-000000000002',
    content_key: 'value',
    content_type: 'application/json',
    content_encoding: 'utf-8',
    content_hash: 'abc123',
    content_hash_algorithm: 'sha256',
    content_size: 42,
    serializer_name: 'labox.json@v1',
    serializer_config: '{}',
    serializer_type: 1,
    storage_name: 'local',
    storage_config: '{}',
    created_at: '2026-01-01T00:00:00Z',
};

afterEach(() => {
    spyOn(globalThis, 'fetch').mockRestore();
});

describe('fetchContentRecord', () => {
    it('calls the correct URL and returns parsed JSON', async () => {
        const spy = spyOn(globalThis, 'fetch').mockResolvedValue(
            new Response(JSON.stringify(RECORD), { status: 200 }),
        );
        const result = await fetchContentRecord(CONTENT_ID, BASE_URL);
        expect(result).toEqual(RECORD);
        expect(spy).toHaveBeenCalledWith(`${BASE_URL}/contents/${CONTENT_ID}`);
    });

    it('throws on a non-ok response', async () => {
        spyOn(globalThis, 'fetch').mockResolvedValue(
            new Response('Not Found', { status: 404, statusText: 'Not Found' }),
        );
        await expect(fetchContentRecord(CONTENT_ID, BASE_URL)).rejects.toThrow(
            'Failed to fetch content record: 404 Not Found',
        );
    });
});

describe('fetchContentData', () => {
    it('calls the correct URL and returns a ReadableStream', async () => {
        const bytes = new Uint8Array([1, 2, 3, 4]);
        const spy = spyOn(globalThis, 'fetch').mockResolvedValue(
            new Response(bytes, { status: 200 }),
        );
        const result = await fetchContentData(CONTENT_ID, BASE_URL);
        expect(result).toBeInstanceOf(ReadableStream);
        const buffer = await new Response(result).arrayBuffer();
        expect(new Uint8Array(buffer)).toEqual(bytes);
        expect(spy).toHaveBeenCalledWith(`${BASE_URL}/contents/${CONTENT_ID}/data`);
    });

    it('throws on a non-ok response', async () => {
        spyOn(globalThis, 'fetch').mockResolvedValue(
            new Response('Internal Server Error', {
                status: 500,
                statusText: 'Internal Server Error',
            }),
        );
        await expect(fetchContentData(CONTENT_ID, BASE_URL)).rejects.toThrow(
            'Failed to fetch content data: 500 Internal Server Error',
        );
    });
});
