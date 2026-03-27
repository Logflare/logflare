import { Page } from '@playwright/test';

const SEARCH_TIMEOUT = 30_000;
const SEARCH_INTERVAL = 3_000;

// Searches for logs and retries until results appear or timeout is reached.
export async function searchLogs(page: Page, searchText: string): Promise<void> {
  const searchInput = page.locator('input[placeholder="Search events"]');
  const table = page.getByRole('table');

  await searchInput.fill(searchText);
  await searchInput.press('Enter');

  const deadline = Date.now() + SEARCH_TIMEOUT;

  while (Date.now() < deadline) {
    await page.waitForTimeout(SEARCH_INTERVAL);

    const text = await table.textContent();

    if (text && !text.includes('No results found')) return;

    await searchInput.fill(searchText);
    await searchInput.press('Enter');
  }
}
