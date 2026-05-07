import { test, expect, type APIRequestContext, type Request } from '@playwright/test';
import { searchLogs } from '../lib/utils';
import supabase from '../lib/supabase';

// Poll the logs.all endpoint until at least one row matches `pattern` in `table`.
// Replaces a fixed sleep that was racy when CI ingestion was slow. The endpoint
// runs the supplied SQL through Logflare via Studio's BFF; `regexp_contains` is
// used because `LIKE` returns 500 against this backend.
async function waitForLogs(
  request: APIRequestContext,
  table: string,
  pattern: string,
  { timeoutMs = 180_000, intervalMs = 1_000 }: { timeoutMs?: number; intervalMs?: number } = {}
): Promise<void> {
  const countSql = `select count(*) as c from ${table} where regexp_contains(event_message, '${pattern}')`;
  const deadline = Date.now() + timeoutMs;
  const startedAt = Date.now();
  let polls = 0;
  let lastCount: number | undefined;
  let lastError: string | undefined;

  while (Date.now() < deadline) {
    polls++;
    const start = new Date(Date.now() - 3_600_000).toISOString();
    const end = new Date(Date.now() + 3_600_000).toISOString();
    const resp = await request.get('/api/platform/projects/default/analytics/endpoints/logs.all', {
      params: { iso_timestamp_start: start, iso_timestamp_end: end, sql: countSql },
    });
    if (resp.ok()) {
      const data = await resp.json().catch(() => null);
      const count = data?.result?.[0]?.c ?? 0;
      lastCount = count;
      if (count > 0) return;
      lastError = undefined;
    } else {
      lastError = `${resp.status()} ${await resp.text().catch(() => '')}`;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }

  // Timed out — fetch a sample of recent rows from the table for forensic context,
  // and log via console.error so the diagnostic surfaces in CI logs (Playwright's
  // hook failures don't fire afterEach, so testInfo.attach isn't available here).
  let sample: string[] = [];
  try {
    const start = new Date(Date.now() - 3_600_000).toISOString();
    const end = new Date(Date.now() + 3_600_000).toISOString();
    const sampleSql = `select event_message from ${table} order by timestamp desc limit 5`;
    const resp = await request.get('/api/platform/projects/default/analytics/endpoints/logs.all', {
      params: { iso_timestamp_start: start, iso_timestamp_end: end, sql: sampleSql },
    });
    if (resp.ok()) {
      const data = await resp.json().catch(() => null);
      sample = (data?.result ?? []).map((r: { event_message?: string }) => (r.event_message ?? '').slice(0, 300));
    }
  } catch (_) { /* ignore — diagnostic only */ }

  const elapsedSec = Math.round((Date.now() - startedAt) / 1000);
  const summary = [
    `waitForLogs(${table}, "${pattern}") timed out after ${timeoutMs}ms`,
    `  elapsed=${elapsedSec}s, polls=${polls}, last count=${lastCount ?? 'n/a'}, last response error=${lastError ?? 'none'}`,
    `  recent ${table} event_messages (${sample.length} rows):`,
    ...sample.map((m, i) => `    [${i}] ${m}`),
  ].join('\n');
  console.error(summary);
  throw new Error(summary);
}

let uniqueId = '';

test.setTimeout(60_000);
expect.configure({ timeout: 15_000 });

type ConsoleEntry = { type: string; text: string; ts: string };
type NetworkEntry = {
  method: string;
  url: string;
  status: number;
  contentType: string;
  durationMs?: number;
  bodyPreview?: string;
  ts: string;
};

const consoleByTest = new Map<string, ConsoleEntry[]>();
const networkByTest = new Map<string, NetworkEntry[]>();

const RELEVANT_URL_PATTERNS = [
  /\/api\/platform\//,
  /\/logs\b/,
  /\/functions\/v1\//,
  /\/auth\/v1\//,
  /\/storage\/v1\//,
  /\/realtime\/v1\//,
  /\/rest\/v1\//,
  /\/pg-meta\//,
];
const isRelevantUrl = (url: string) => RELEVANT_URL_PATTERNS.some(re => re.test(url));
const MAX_BODY_PREVIEW = 4_000;

test.beforeEach(async ({ page }, testInfo) => {
  const messages: ConsoleEntry[] = [];
  consoleByTest.set(testInfo.testId, messages);

  page.on('console', msg => {
    messages.push({ type: msg.type(), text: msg.text(), ts: new Date().toISOString() });
  });
  page.on('pageerror', err => {
    messages.push({
      type: 'pageerror',
      text: `${err.name}: ${err.message}${err.stack ? '\n' + err.stack : ''}`,
      ts: new Date().toISOString(),
    });
  });

  const network: NetworkEntry[] = [];
  networkByTest.set(testInfo.testId, network);
  const requestStart = new WeakMap<Request, number>();

  page.on('request', req => {
    if (isRelevantUrl(req.url())) requestStart.set(req, Date.now());
  });

  page.on('response', async resp => {
    const url = resp.url();
    if (!isRelevantUrl(url)) return;
    const req = resp.request();
    const startedAt = requestStart.get(req);
    const contentType = resp.headers()['content-type'] ?? '';

    let bodyPreview: string | undefined;
    if (contentType.includes('json') || contentType.includes('text')) {
      try {
        const body = await resp.text();
        bodyPreview = body.length > MAX_BODY_PREVIEW
          ? body.slice(0, MAX_BODY_PREVIEW) + `…[truncated ${body.length - MAX_BODY_PREVIEW} chars]`
          : body;
      } catch {
        bodyPreview = '<unable to read body>';
      }
    }

    network.push({
      method: req.method(),
      url,
      status: resp.status(),
      contentType,
      durationMs: startedAt ? Date.now() - startedAt : undefined,
      bodyPreview,
      ts: new Date().toISOString(),
    });
  });
});

test.afterEach(async ({ page }, testInfo) => {
  const messages = consoleByTest.get(testInfo.testId) ?? [];
  const network = networkByTest.get(testInfo.testId) ?? [];
  consoleByTest.delete(testInfo.testId);
  networkByTest.delete(testInfo.testId);

  if (testInfo.status === testInfo.expectedStatus) return;

  try {
    const tableHtml = await page.getByRole('table').innerHTML({ timeout: 1_000 });
    await testInfo.attach('table.html', { body: tableHtml, contentType: 'text/html' });
  } catch (e) {
    await testInfo.attach('table-error.txt', {
      body: `Failed to capture table HTML: ${(e as Error).message}`,
      contentType: 'text/plain',
    });
  }

  try {
    const tableText = await page.getByRole('table').textContent({ timeout: 1_000 });
    await testInfo.attach('table.txt', {
      body: tableText ?? '',
      contentType: 'text/plain',
    });
  } catch (e) {
    await testInfo.attach('table-text-error.txt', {
      body: `Failed to capture table text: ${(e as Error).message}`,
      contentType: 'text/plain',
    });
  }

  await testInfo.attach('browser-console.json', {
    body: JSON.stringify(messages, null, 2),
    contentType: 'application/json',
  });

  await testInfo.attach('network.json', {
    body: JSON.stringify(network, null, 2),
    contentType: 'application/json',
  });

  await testInfo.attach('page-url.txt', {
    body: page.url(),
    contentType: 'text/plain',
  });
});

test.beforeAll(async ({ request, browserName }) => {
  // The polling below can take up to ~180s in worst case; bump the hook timeout
  // to give it headroom on top of the synchronous setup work above.
  test.setTimeout(240_000);

  uniqueId = `${Date.now()}_${browserName}`;

  await supabase.auth.signUp({ email: `example_${uniqueId}@email.com`, password: 'example-password' })

  await supabase.storage.createBucket(`avatars_${uniqueId}`)
  await supabase.storage.deleteBucket(`avatars_${uniqueId}`)

  await supabase.functions.invoke(`function_${uniqueId}`)
  await supabase.functions.invoke('hello')

  // Realtime
  const channel = supabase.channel(`test_${uniqueId}`)
  channel.subscribe()
  await new Promise(r => setTimeout(r, 2000))
  await supabase.removeChannel(channel)

  // PostgRest
  await request.post('/api/platform/pg-meta/default/query?key=table-create-with-columns', { data: {
    disable_statement_timeout: false,
    query: `BEGIN; CREATE TABLE public."table_test_${uniqueId}" (); COMMENT ON TABLE public."table_test_${uniqueId}" IS ''; COMMIT;;\nALTER TABLE "public"."table_test_${uniqueId}" ENABLE ROW LEVEL SECURITY;\n\nBEGIN;\n  ALTER TABLE public."table_test_${uniqueId}" ADD COLUMN id int8\n    GENERATED BY DEFAULT AS IDENTITY\n    \n    \n    \n    ;\n  ;\nCOMMIT;;\n\nBEGIN;\n  ALTER TABLE public."table_test_${uniqueId}" ADD COLUMN created_at timestamptz\n    DEFAULT now()\n    NOT NULL\n    \n    \n    ;\n  ;\nCOMMIT;;\nALTER TABLE "public"."table_test_${uniqueId}" ADD PRIMARY KEY ("id")`
  }});

  // Cron jobs
  await request.post('/api/platform/pg-meta/default/query?key=extension-create', { data: {
    disable_statement_timeout: false,
    query: "\nCREATE EXTENSION IF NOT EXISTS pg_cron\n  SCHEMA pg_catalog\n  VERSION '1.6'\n  CASCADE;"
  }});

  await request.post('/api/platform/pg-meta/default/query?key=cron-jobs-create', { data: {
    disable_statement_timeout: false,
    query: `select cron.schedule('test_cron_${uniqueId}', '5 seconds', $$SELECT auth.email()$$);`
  }});

  // Wait for ingestion of THIS test's setup events, in parallel. These are
  // uniqueId-tagged so we can confirm our specific data made it through the
  // (Vector → Logflare → backend → searchable) path before the test bodies
  // run their searches. Replaces a fixed 30s sleep that was racy on slow CI
  // runners.
  //
  // The other pipelines (postgrest "Config reloaded", realtime "Billing
  // metrics", edge functions /home/deno/functions/hello, cron job)
  // emit periodically or on infra events independent of our setup; the test
  // bodies' date-windowed searches reliably hit historical entries, so we
  // don't need to poll those in beforeAll.
  await Promise.all([
    waitForLogs(request, 'edge_logs', `function_${uniqueId}`),
    waitForLogs(request, 'auth_logs', `example_${uniqueId}`),
    waitForLogs(request, 'storage_logs', `avatars_${uniqueId}`),
  ]);
});

test.afterAll(async ({ request }) => {
  await request.post('/api/platform/pg-meta/default/query?key=cron-jobs-unschedule', { data: {
    disable_statement_timeout: false,
    query: `select cron.unschedule('test_cron_${uniqueId}');`
  }});
});

test('receives logs from API Gateway', async ({ page }) => {
  await page.goto('/project/default/logs/edge-logs');
  await searchLogs(page, `/functions/v1/function_${uniqueId}`);

  await expect(page.getByRole('table')).toContainText(`/functions/v1/function_${uniqueId}`);
});

test('receives logs from PostgREST', async ({ page }) => {
  await page.goto('/project/default/logs/postgrest-logs');
  await searchLogs(page, 'Config reloaded');

  await expect(page.getByRole('table')).toContainText('Config reloaded');
});

test('receives logs from Auth', async ({ page }) => {
  await page.goto('/project/default/logs/auth-logs');
  await searchLogs(page, `example_${uniqueId}@email.com`);

  await expect(page.getByRole('table')).toContainText('/signup | request completed');
});

test('receives logs from Storage', async ({ page }) => {
  await page.goto('/project/default/logs/storage-logs');
  await searchLogs(page, `/bucket/avatars_${uniqueId}`);

  await expect(page.getByRole('table')).toContainText(`/bucket/avatars_${uniqueId}`);
});

test('receives logs from Realtime', async ({ page }) => {
  await page.goto('/project/default/logs/realtime-logs');
  await searchLogs(page, 'Billing');

  await expect(page.getByRole('table')).toContainText('Billing metrics');
});

test('receives logs from Edge Functions', async ({ page }) => {
  await page.goto('/project/default/logs/edge-functions-logs');
  await searchLogs(page, '/home/deno/functions/hello');

  await expect(page.getByRole('table')).toContainText('serving the request with /home/deno/functions/hello');
});

test('receives logs from Cron', async ({ page }) => {
  await page.goto('/project/default/logs/pgcron-logs');

  await expect(page.getByRole('table')).toContainText(/LOG:\s+cron job \d+ completed: 1 row/);
  await expect(page.getByRole('table')).toContainText(/LOG:\s+cron job \d+ starting: SELECT auth\.email\(\)/);
});
