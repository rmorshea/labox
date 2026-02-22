import { useEffect, useRef } from 'preact/hooks';
import type { Renderer } from './types';
import type { ContentRecord } from '../types';

function ImageView({ data, record }: { data: ArrayBuffer; record: ContentRecord }) {
    const imgRef = useRef<HTMLImageElement>(null);

    useEffect(() => {
        const blob = new Blob([data], { type: record.content_type });
        const url = URL.createObjectURL(blob);
        const img = imgRef.current;
        if (img) img.src = url;
        return () => URL.revokeObjectURL(url);
    }, [data, record.content_type]);

    return (
        <div class="labox-content--image">
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
