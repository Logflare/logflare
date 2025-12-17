import { test, expect } from '@playwright/test';

test.beforeAll(async ({ request }) => {
  await request.get('/api/platform/organizations');

  await request.post('/api/platform/storage/default/buckets', { data: {id: "bucket", type: "STANDARD", public: false}});
  await request.post('/storage/v1/object/bucket/folder/.emptyFolderPlaceholder');

  await new Promise(r => setTimeout(r, 10000))
});

test('receives logs from API Gateway', async ({ page }) => {
  await page.goto('/project/default/logs/edge-logs');
  await page.fill('input[placeholder="Search events"]', '/api/platform/organizations');
  await page.press('input[placeholder="Search events"]', 'Enter');

  await expect(page.getByRole('table')).toContainText('/api/platform/organizations');
});

test('receives logs from Storage', async ({ page }) => {
  await page.goto('/project/default/logs/storage-logs');
  await page.fill('input[placeholder="Search events"]', 'folder');
  await page.press('input[placeholder="Search events"]', 'Enter');

  await expect(page.getByRole('table')).toContainText('/object/bucket/folder/.emptyFolderPlaceholder');
});