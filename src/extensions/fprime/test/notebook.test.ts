import { expect, jest, test } from '@jest/globals';

// Jest swaps 'vscode' for the stub in src/fallback/vscode, which has no notebook classes, so we mock the ones
// the serializer uses.
jest.mock('vscode', () => ({
    NotebookCellKind: { Markup: 1, Code: 2 },
    NotebookData: class { constructor(readonly cells: object[]) { } },
    NotebookCellData: class { constructor(readonly kind: number, public value: string, readonly languageId: string) { } },
    NotebookCellOutput: class { constructor(readonly items: object[]) { } },
    NotebookCellOutputItem: class { constructor(readonly data: Uint8Array, readonly mime: string) { } },
}));

import * as vscode from 'vscode';
import { FPrimeNotebookSerializer } from '../src/notebook';

test('saves each command output on one line, and loads it back into its cell after the cell\'s events', async () => {
    const output = (text: string, mime: string) => new vscode.NotebookCellOutput([
        new vscode.NotebookCellOutputItem(new TextEncoder().encode(text), mime),
    ]);
    const commands = ['CMD_A  ✓ completed · 0.1 s\n✓ Sent     2026-10-14T20:46:47.631Z', 'CMD_B  ✗ failed · EXECUTION_ERROR'];
    const cells = ['CMD_A\nCMD_B', 'CMD_C'].map((text) => new vscode.NotebookCellData(vscode.NotebookCellKind.Code, text, 'fprime'));
    cells[0].outputs = [output('', 'hermes.notebook/evr'), ...commands.map((text) => output(text, 'hermes.notebook/command'))];
    const token: vscode.CancellationToken = { isCancellationRequested: false, onCancellationRequested: () => ({ dispose: () => { } }) };

    const serializer = new FPrimeNotebookSerializer();
    const saved = await serializer.serializeNotebook(new vscode.NotebookData(cells), token);
    const loaded = await serializer.deserializeNotebook(saved, token);

    expect(new TextDecoder().decode(saved).split('\n').filter((line) => line.startsWith(';?c'))).toHaveLength(2);
    expect(loaded.cells.map((cell) => cell.value)).toEqual(['CMD_A\nCMD_B', 'CMD_C']);
    // Every cell loads with a list of events, so the second cell gets an empty one and no command outputs
    const outputs = loaded.cells.map((cell) => cell.outputs?.map(({ items: [item] }) => [item.mime, new TextDecoder().decode(item.data)]));
    expect(outputs).toEqual([
        [['hermes.notebook/evr', ''], ...commands.map((text) => ['hermes.notebook/command', text])],
        [['hermes.notebook/evr', '']],
    ]);
});
