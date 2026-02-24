import { test, expect, Page, Locator } from '@playwright/test';
import { searchLogs } from '../lib/utils';
import supabase from '../lib/supabase';

const LOG_WAIT_TIME = 8000;
let functionName = '';

test.beforeEach(async ({ page, browserName }) => {
  functionName = `function_${Date.now()}_${browserName}`;

  await supabase.functions.invoke(functionName, { body: { name: 'Functions' } });

  await page.waitForTimeout(LOG_WAIT_TIME);
});

test('verifies clipboard functionality for log detail fields (id, status, timestamp, method, path)', async ({ page, browserName }) => {
  await page.goto('/project/default/logs/edge-logs');

  await expect(page.locator('.recharts-surface')).toBeVisible();

  await searchLogs(page, functionName);

  await page.locator('.recharts-rectangle.cursor-pointer').first().hover();
  await expect(page.locator('.recharts-tooltip-wrapper')).toBeVisible();

  await page.getByRole('gridcell', { name: functionName }).first().click();

  const fieldsToTest = [
    { locator: () => page.getByTestId('log-selection-id'), label: 'Copy id' },
    { locator: () => page.getByTestId('log-selection').getByRole('button', { name: 'status' }), label: 'Copy status' },
    { locator: () => page.getByRole('button', { name: 'timestamp' }), label: 'Copy timestamp' },
    { locator: () => page.getByRole('button', { name: 'method POST' }), label: 'Copy method' },
    { locator: () => page.getByRole('button', { name: 'path /functions/v1/' }), label: 'Copy path' }
  ];

  for (const field of fieldsToTest) {
    await expectClipboardViaPaste(page, browserName, field.locator(), field.label);
  }

  const expandButton = page.getByRole('button', { name: 'Expand' });

  await expandButton.click();

  const metadata = page.getByText('[ { "request": [ { "headers').first();

  await expect(metadata).toContainText('"method": "POST",');
  await expect(metadata).toContainText(`"path": "/functions/v1/${functionName}",`);
  await expect(metadata).toContainText('"status_code": 500');
});

async function expectClipboardViaPaste(page: Page, browserName: string, selection: Locator, menuItemName: string): Promise<void> {
  const expectedText = (await selection.innerText()).split('\n').map(line => line.trim()).filter(Boolean).at(-1);
  if (!expectedText) { throw new Error('No text found in selection') }

  const expectedClipboard = menuItemName === 'Copy timestamp' ? parseToMicroseconds(expectedText) : expectedText;

  await selection.hover();
  await selection.click({ trial: true });
  await selection.click();

  const menuItem = page.getByRole('menuitem', { name: menuItemName });
  await expect(menuItem).toBeVisible();
  await menuItem.click();

  switch(browserName) {
    case 'chromium':
      await chromiumClipboardReadText(page, expectedClipboard);
      break;

    default:
      await defaultClipboardReadText(page, expectedClipboard);
  }
}

function parseToMicroseconds(dateStr: string): string {
  const microseconds = new Date(dateStr).getTime() * 1000;
  return microseconds.toString();
}

async function chromiumClipboardReadText(page: Page, expectedText: string) {
  await expect.poll(async () => {
    return await page.evaluate(() => navigator.clipboard.readText());
  }).toBe(expectedText);
}

async function defaultClipboardReadText(page: Page, expectedText: string) {
  const pasteTarget = page.getByRole('textbox', { name: 'Search collections...' });
  await pasteTarget.fill('');
  await pasteTarget.focus();

  const modifier = process.platform === 'darwin' ? 'Meta' : 'Control';
  await page.keyboard.press(`${modifier}+V`);

  await expect(pasteTarget).toHaveValue(expectedText);
}