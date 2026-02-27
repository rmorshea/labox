import { useState, useEffect } from 'preact/hooks';
import { readAll } from './utils';
import type { Renderer } from './types';

function JsonView({ data }: { data: ReadableStream<Uint8Array> }) {
    const [formatted, setFormatted] = useState<string | null>(null);

    useEffect(() => {
        readAll(data).then(buffer => {
            const text = new TextDecoder().decode(buffer);
            try {
                setFormatted(JSON.stringify(JSON.parse(text), null, 2));
            } catch {
                setFormatted(text);
            }
        });
    }, [data]);

    if (formatted === null) {
        return <div class="labox-loading" />;
    }

    return (
        <div class="labox-content--json">
            <pre class="labox-json">
                <code>{formatted}</code>
            </pre>
        </div>
    );
}

export const jsonRenderer: Renderer = {
    types: ['application/json'],
    render(data) {
        return <JsonView data={data} />;
    },
};
