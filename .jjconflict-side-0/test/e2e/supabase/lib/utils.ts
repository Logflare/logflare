import { Page } from '@playwright/test';

const MAX_RETRIES = 10;
const RETRY_INTERVAL = 1_000;

// Searches for logs and retries until results appear or max retries is reached.
// If expectedText is provided, retries until the table contains that specific text.
export async function searchLogs(page: Page, searchText: string): Promise<void> {
  const searchInput = page.locator('input[placeholder="Search events"]');
  const table = page.getByRole('table');

  for (let i = 0; i < MAX_RETRIES; i++) {
    await searchInput.fill(searchText);
    await searchInput.press('Enter');
    await page.waitForTimeout(RETRY_INTERVAL);

    const text = await table.textContent();

    if (text && !text.includes('No results found') && text.includes(searchText)) return;
  }
}
