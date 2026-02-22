import type { Renderer } from './types';
import type { ContentRecord } from '../types';

export const jsonRenderer: Renderer = {
    types: ['application/json'],

    render(data: ArrayBuffer, _record: ContentRecord) {
        const text = new TextDecoder().decode(data);
        let formatted: string;
        try {
            formatted = JSON.stringify(JSON.parse(text), null, 2);
        } catch {
            formatted = text;
        }
        return (
            <div class="labox-content labox-content--json">
                <pre class="labox-json">
                    <code>{formatted}</code>
                </pre>
            </div>
        );
    },
};
