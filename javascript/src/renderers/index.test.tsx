import { describe, it, expect } from 'bun:test';
import {
    buildRendererMap,
    findRenderer,
    fallbackRenderer
} from './index';
import { jsonRenderer } from './json';
import { csvRenderer } from './csv';
import { imageRenderer } from './image';
import { plotlyRenderer } from './plotly';
import type { Renderer } from './types';

describe('findRenderer', () => {
    it('returns the exact renderer for a known content type', () => {
        expect(findRenderer('application/json')).toBe(jsonRenderer);
        expect(findRenderer('text/csv')).toBe(csvRenderer);
        expect(findRenderer('image/webp')).toBe(imageRenderer);
        expect(findRenderer('application/vnd.plotly.v1+json')).toBe(plotlyRenderer);
    });

    it('falls back via structured-syntax suffix (+json → application/json)', () => {
        // A hypothetical vendor type whose suffix is "json"
        expect(findRenderer('application/vnd.unknown+json')).toBe(jsonRenderer);
    });

    it('returns fallbackRenderer for an unknown type with no suffix', () => {
        expect(findRenderer('application/octet-stream')).toBe(fallbackRenderer);
    });

    it('returns fallbackRenderer for an unknown type with an unrecognised suffix', () => {
        expect(findRenderer('application/vnd.foo+msgpack')).toBe(fallbackRenderer);
    });

    it('uses a custom map when provided', () => {
        const custom: Renderer = { types: ['text/plain'], render: () => <div /> };
        const map = buildRendererMap([custom]);
        expect(findRenderer('text/plain', map)).toBe(custom);
        // Types not in the custom map fall back to fallbackRenderer
        expect(findRenderer('application/json', map)).toBe(fallbackRenderer);
    });
});
