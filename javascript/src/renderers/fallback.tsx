import type { Renderer } from './types';
import type { ContentRecord } from '../types';

export const fallbackRenderer: Renderer = {
    types: [],

    render(_data: ArrayBuffer, record: ContentRecord) {
        return (
            <div class="labox-content labox-content--fallback">
                <dl class="labox-fallback">
                    <dt>Content type</dt>
                    <dd>{record.content_type}</dd>
                    <dt>Content key</dt>
                    <dd>{record.content_key}</dd>
                    <dt>Size</dt>
                    <dd>{record.content_size.toLocaleString()} bytes</dd>
                </dl>
            </div>
        );
    },
};
