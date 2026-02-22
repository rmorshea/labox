import { useState, useEffect } from 'preact/hooks';
import { readAll } from './utils';
import type { Renderer } from './types';
import type { ContentRecord } from '../types';

function CsvTable({ data }: { data: ReadableStream<Uint8Array> }) {
    const [rows, setRows] = useState<string[][] | null>(null);

    useEffect(() => {
        Promise.all([import('papaparse'), readAll(data)]).then(([{ default: Papa }, buffer]) => {
            const text = new TextDecoder().decode(buffer);
            const { data: parsed } = Papa.parse<string[]>(text, { skipEmptyLines: true });
            setRows(parsed);
        });
    }, [data]);

    if (rows === null) {
        return <div class="labox-loading" />;
    }

    const [header, ...body] = rows;

    return (
        <div class="labox-content--csv">
            <table class="labox-csv-table">
                {header && (
                    <thead class="labox-csv-thead">
                        <tr class="labox-csv-tr">
                            {header.map((cell, i) => (
                                <th key={i} class="labox-csv-th">
                                    {cell}
                                </th>
                            ))}
                        </tr>
                    </thead>
                )}
                <tbody class="labox-csv-tbody">
                    {body.map((row, ri) => (
                        <tr key={ri} class="labox-csv-tr">
                            {row.map((cell, ci) => (
                                <td key={ci} class="labox-csv-td">
                                    {cell}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export const csvRenderer: Renderer = {
    types: ['text/csv'],

    render(data: ReadableStream<Uint8Array>, _record: ContentRecord) {
        return <CsvTable data={data} />;
    },
};
