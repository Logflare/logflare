import { Page  } from '@playwright/test';

export async function searchLogs(page: Page, searchText: string): Promise<void> {
  const searchInput = page.locator('input[placeholder="Search events"]');

  await searchInput.fill(searchText);
  await searchInput.press('Enter');
}