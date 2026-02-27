/**
 * Read a {@link ReadableStream} to completion and return the result as an
 * {@link ArrayBuffer}. Useful for renderers that need the full payload before
 * they can display anything.
 */
export function readAll(stream: ReadableStream<Uint8Array>): Promise<ArrayBuffer> {
    return new Response(stream).arrayBuffer();
}
