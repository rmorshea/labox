import type { Renderer } from './types';
import type { ContentRecord } from '../types';

function parseCSV(text: string): string[][] {
    return text
        .split(/\r?\n/)
        .filter((line) => line.length > 0)
        .map((line) => line.split(','));
}

export const csvRenderer: Renderer = {
    types: ['text/csv'],

    render(data: ArrayBuffer, _record: ContentRecord) {
        const text = new TextDecoder().decode(data);
        const rows = parseCSV(text);
        const [header, ...body] = rows;

        return (
            <div class="labox-content labox-content--csv">
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
    },
};
