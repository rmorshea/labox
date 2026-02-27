import { useEffect, useRef, useState } from 'preact/hooks';
import { readAll } from './utils';
import type { Renderer } from './types';
import type { ContentRecord } from '../types';

function ImageView({ data, record }: { data: ReadableStream<Uint8Array>; record: ContentRecord }) {
    const imgRef = useRef<HTMLImageElement>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let url: string | null = null;
        readAll(data).then(buffer => {
            const blob = new Blob([buffer], { type: record.content_type });
            url = URL.createObjectURL(blob);
            const img = imgRef.current;
            if (img) img.src = url;
            setLoading(false);
        });
        return () => { if (url) URL.revokeObjectURL(url); };
    }, [data, record.content_type]);

    return (
        <div class="labox-content--image">
            {loading && <div class="labox-loading" />}
            <img class="labox-image" ref={imgRef} alt={record.content_key} />
        </div>
    );
}

export const imageRenderer: Renderer = {
    types: [
        'image/png',
        'image/jpeg',
        'image/gif',
        'image/webp',
        'image/svg+xml',
        'image/bmp',
        'image/tiff',
        'image/avif',
        'image/apng',
        'image/x-icon',
    ],

    render(data, record) {
        return <ImageView data={data} record={record} />;
    },
};
