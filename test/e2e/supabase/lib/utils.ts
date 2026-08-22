import { Page } from '@playwright/test';

const MAX_RETRIES = 20;
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

  // Returning quietly here pushes the failure onto whatever the caller does
  // next (e.g. hovering a chart bar that was never rendered), which surfaces as
  // an unrelated locator timeout. Fail where the problem actually is.
  throw new Error(
    `searchLogs: "${searchText}" did not appear in the results table after ` +
      `${(MAX_RETRIES * RETRY_INTERVAL) / 1_000}s — the event was never ingested or is not searchable.`,
  );
}
