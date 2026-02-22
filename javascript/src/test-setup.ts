/**
 * Bun test preload — mocks plotly.js-dist-min so that importing it in a
 * non-browser (Node-like) environment doesn't crash.  Tests only exercise
 * renderer registration/lookup, not actual rendering, so a no-op stub suffices.
 */
import { mock } from 'bun:test';

const plotlyStub = {
    newPlot: () => Promise.resolve(),
    purge: () => {},
    react: () => Promise.resolve(),
    update: () => Promise.resolve(),
};

mock.module('plotly.js-dist-min', () => ({
    default: plotlyStub,
    ...plotlyStub,
}));
