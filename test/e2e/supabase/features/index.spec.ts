import { test, expect } from '@playwright/test';

test('has title', async ({ page }) => {
  await page.goto('/');

  await expect(page).toHaveTitle(/Logflare | Cloudflare, Vercel & Elixir Logging/);
});
