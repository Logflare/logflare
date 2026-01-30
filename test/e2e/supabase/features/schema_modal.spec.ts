import { test, expect, Page, Locator } from '@playwright/test';
import { searchLogs } from '../lib/utils';
import supabase from '../lib/supabase';

let functionName = '';

test.beforeEach(async ({ browserName }) => {
  functionName = `function_${Date.now()}_${browserName}`;

  await supabase.functions.invoke(functionName, { body: { name: 'Functions' } });

  await new Promise(r => setTimeout(r, 8000));
});

test('copies correct log id and status to clipboard', async ({ page, browserName }) => {
  await page.goto('/project/default/logs/edge-logs');

  await expect(page.locator('.recharts-surface')).toBeVisible();

  await searchLogs(page, functionName);

  await page.locator('.recharts-rectangle.cursor-pointer').first().hover();
  await expect(page.locator('.recharts-tooltip-wrapper')).toBeVisible();

  await page.getByRole('gridcell', { name: functionName }).first().click();

  const idSelection = page.getByTestId('log-selection-id');
  await expectClipboardViaPaste(page, browserName, idSelection, 'Copy id');
});

async function expectClipboardViaPaste(page: Page, browserName: string, selection: Locator, menuItemName: string): Promise<void> {
  const expectedText = (await selection.innerText()).split('\n').map(line => line.trim()).filter(Boolean).at(-1);
  if (!expectedText) { throw new Error('No text found in selection') }

  await selection.click();

  const menuItem = page.getByRole('menuitem', { name: menuItemName })
  await menuItem.waitFor({ state: 'visible' });
  await menuItem.click();

  switch(browserName) {
    case 'chromium':
      await chromiumClipboardReadText(page, expectedText);
      break;

    default:
      await defaultClipboardReadText(page, expectedText);
  }
}

async function chromiumClipboardReadText(page: Page, expectedText: string) {
  await expect(page.getByText('Copied to clipboard')).toBeVisible();

  const clipboardText = await page.evaluate(async () => { return await navigator.clipboard.readText(); });

  expect(clipboardText).toBe(expectedText);
}

async function defaultClipboardReadText(page: Page, expectedText: string) {
  const pasteTarget = page.locator('input[placeholder="Search events"]');
  await pasteTarget.fill('');
  await pasteTarget.focus();

  const modifier = process.platform === 'darwin' ? 'Meta' : 'Control';
  await page.keyboard.press(`${modifier}+V`);

  await expect(pasteTarget).toHaveValue(expectedText);
}