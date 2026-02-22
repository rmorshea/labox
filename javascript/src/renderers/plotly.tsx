import { useEffect, useRef, useState } from 'preact/hooks';
import type * as PlotlyTypes from 'plotly.js';
import { readAll } from './utils';
import type { Renderer } from './types';
import type { ContentRecord } from '../types';

interface PlotlyFigure {
    data?: PlotlyTypes.Data[];
    layout?: Partial<PlotlyTypes.Layout>;
    config?: Partial<PlotlyTypes.Config>;
}

function PlotlyView({ data, record }: { data: ReadableStream<Uint8Array>; record: ContentRecord }) {
    const divRef = useRef<HTMLDivElement>(null);
    const [ready, setReady] = useState(false);

    useEffect(() => {
        const el = divRef.current;
        if (!el) return;

        let cancelled = false;

        Promise.all([import('plotly.js'), readAll(data)]).then(([{ default: Plotly }, buffer]) => {
            if (cancelled || !divRef.current) return;
            const text = new TextDecoder().decode(buffer);
            let fig: PlotlyFigure;
            try {
                fig = JSON.parse(text) as PlotlyFigure;
            } catch {
                return;
            }
            Plotly.newPlot(el, fig.data ?? [], fig.layout ?? {}, {
                responsive: true,
                ...fig.config,
            });
            setReady(true);
        });

        return () => {
            cancelled = true;
            import('plotly.js').then(({ default: Plotly }) => Plotly.purge(el));
        };
    }, [data, record.content_key]);

    return (
        <div class="labox-content--plotly">
            {!ready && <div class="labox-loading" />}
            <div class="labox-plotly" ref={divRef} />
        </div>
    );
}

export const plotlyRenderer: Renderer = {
    types: ['application/vnd.plotly.v1+json'],

    render(data, record) {
        return <PlotlyView data={data} record={record} />;
    },
};
