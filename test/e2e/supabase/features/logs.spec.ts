import { test, expect } from '@playwright/test';

test.beforeAll(async ({ request }) => {
  await request.get('/api/platform/organizations');
  await new Promise(r => setTimeout(r, 5000))
});

test('receives logs from API Gateway', async ({ page }) => {
  await page.goto('/project/default/logs/edge-logs');
  await page.fill('input[placeholder="Search events"]', '/api/platform/organizations');
  await page.press('input[placeholder="Search events"]', 'Enter');

  await expect(page.getByRole('table')).toContainText('/api/platform/organizations');
});
