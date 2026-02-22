import { useEffect, useRef } from 'preact/hooks';
import type { Renderer } from './types';
import type { ContentRecord } from '../types';

function PlotlyFigure({ data, record }: { data: ArrayBuffer; record: ContentRecord }) {
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!containerRef.current) return;
        const el = containerRef.current;
        const text = new TextDecoder().decode(data);
        let cancelled = false;

        import('plotly.js').then((Plotly) => {
            if (cancelled || !el) return;
            const figure = JSON.parse(text) as { data: object[]; layout: object };
            void Plotly.newPlot(el, figure.data as Plotly.Data[], figure.layout as Partial<Plotly.Layout>);
        });

        return () => {
            cancelled = true;
        };
        // record is used for keying only; data is the actual dependency
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data, record.id]);

    return (
        <div class="labox-content labox-content--plotly">
            <div class="labox-plotly" ref={containerRef} />
        </div>
    );
}

export const plotlyRenderer: Renderer = {
    types: ['application/vnd.plotly.v1+json'],

    render(data, record) {
        return <PlotlyFigure data={data} record={record} />;
    },
};
