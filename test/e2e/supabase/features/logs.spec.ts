import { test, expect, type APIRequestContext, type Page, type Request, type TestInfo } from '@playwright/test';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { searchLogs } from '../lib/utils';
import supabase from '../lib/supabase';

const execFileP = promisify(execFile);

// Mapping from logs.all CTE alias to the underlying Logflare source name.
// Kept in sync with priv/supabase/endpoints/logs.all.sql — the SQL there
// aliases each `from \`<source.name>\`` block, and tests below pass those
// aliases through waitForLogs. Only used by sampleFromPostgres.
const SOURCE_NAME_BY_TABLE: Record<string, string> = {
  edge_logs: 'cloudflare.logs.prod',
  postgres_logs: 'postgres.logs',
  function_edge_logs: 'deno-relay-logs',
  function_logs: 'deno-subhosting-events',
  auth_logs: 'gotrue.logs.prod',
  realtime_logs: 'realtime.logs.prod',
  storage_logs: 'storage.logs.prod.2',
  postgrest_logs: 'postgREST.logs.prod',
  pgbouncer_logs: 'pgbouncer.logs.prod',
};

// Fetch the last `limit` event_messages directly from the analytics Postgres
// backend via `docker exec`. Bypasses Logflare's HTTP/SQL endpoint stack so
// the diagnostic still works when those layers are themselves the failure
// mode (e.g. logs.all returning 5xx or hanging). Best-effort — returns an
// empty array on any error, since this only runs on the unhappy path.
async function sampleFromPostgres(table: string, limit = 5): Promise<string[]> {
  const sourceName = SOURCE_NAME_BY_TABLE[table];
  const password = process.env.POSTGRES_PASSWORD;
  if (!sourceName || !password) return [];

  const psql = async (sql: string) => {
    const { stdout } = await execFileP(
      'docker',
      [
        'exec',
        '-e', `PGPASSWORD=${password}`,
        'supabase-db',
        'psql', '-U', 'supabase_admin', '-d', '_supabase',
        '-t', '-A', '-c', sql,
      ],
      { timeout: 5_000 },
    );
    return stdout;
  };

  try {
    const tokenOut = await psql(
      `select token from _analytics.sources where name = '${sourceName}' limit 1`,
    );
    const token = tokenOut.trim();
    if (!token) return [];
    const physicalTable = `log_events_${token.replace(/-/g, '_')}`;
    const sampleOut = await psql(
      `select event_message from _analytics.${physicalTable} order by timestamp desc limit ${limit}`,
    );
    return sampleOut.split('\n').filter(Boolean).map(l => l.slice(0, 300));
  } catch {
    return [];
  }
}

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

  // Timed out — fetch a sample of recent rows directly from the analytics
  // Postgres backend for forensic context. Going via psql (rather than the
  // same logs.all endpoint the polling loop just exhausted) means this still
  // surfaces useful data when the endpoint itself is the failure mode.
  // Logged via console.error so the diagnostic surfaces in CI logs (Playwright's
  // hook failures don't fire afterEach, so testInfo.attach isn't available here).
  const sample = await sampleFromPostgres(table);

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

type ConsoleEntry = { type: string; text: string; ts: string };
type NetworkEntry = {
  method: string;
  url: string;
  status: number;
  contentType: string;
  durationMs?: number;
  ts: string;
};
type Diagnostics = { console: ConsoleEntry[]; network: NetworkEntry[] };

const diagnosticsByTest = new Map<string, Diagnostics>();

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

async function captureTable(page: Page, testInfo: TestInfo, kind: 'html' | 'text') {
  const filename = kind === 'html' ? 'table.html' : 'table.txt';
  const contentType = kind === 'html' ? 'text/html' : 'text/plain';
  try {
    const locator = page.getByRole('table');
    const body = kind === 'html'
      ? await locator.innerHTML({ timeout: 1_000 })
      : (await locator.textContent({ timeout: 1_000 })) ?? '';
    await testInfo.attach(filename, { body, contentType });
  } catch (e) {
    await testInfo.attach(`${filename}.error.txt`, {
      body: `Failed to capture table ${kind}: ${(e as Error).message}`,
      contentType: 'text/plain',
    });
  }
}

test.beforeEach(async ({ page }, testInfo) => {
  const consoleEntries: ConsoleEntry[] = [];
  const network: NetworkEntry[] = [];
  diagnosticsByTest.set(testInfo.testId, { console: consoleEntries, network });

  page.on('console', msg => {
    consoleEntries.push({ type: msg.type(), text: msg.text(), ts: new Date().toISOString() });
  });
  page.on('pageerror', err => {
    consoleEntries.push({
      type: 'pageerror',
      text: `${err.name}: ${err.message}${err.stack ? '\n' + err.stack : ''}`,
      ts: new Date().toISOString(),
    });
  });

  const requestStart = new WeakMap<Request, number>();

  page.on('request', req => {
    if (isRelevantUrl(req.url())) requestStart.set(req, Date.now());
  });

  page.on('response', resp => {
    const url = resp.url();
    if (!isRelevantUrl(url)) return;
    const req = resp.request();
    const startedAt = requestStart.get(req);

    network.push({
      method: req.method(),
      url,
      status: resp.status(),
      contentType: resp.headers()['content-type'] ?? '',
      durationMs: startedAt ? Date.now() - startedAt : undefined,
      ts: new Date().toISOString(),
    });
  });
});

test.afterEach(async ({ page }, testInfo) => {
  const diag = diagnosticsByTest.get(testInfo.testId) ?? { console: [], network: [] };
  diagnosticsByTest.delete(testInfo.testId);

  if (testInfo.status === testInfo.expectedStatus) return;

  await captureTable(page, testInfo, 'html');
  await captureTable(page, testInfo, 'text');

  await testInfo.attach('browser-console.json', {
    body: JSON.stringify(diag.console, null, 2),
    contentType: 'application/json',
  });

  await testInfo.attach('network.json', {
    body: JSON.stringify(diag.network, null, 2),
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

  // The Supabase JS SDK returns { data, error } and never throws on HTTP
  // failures. Log SDK errors as forensic context but don't fail the hook on
  // them: some errors (e.g. auth's "Error sending confirmation email") are
  // downstream of the request that produces the log we care about, so the
  // waitForLogs polls below remain the source of truth on whether setup
  // actually populated the index.
  const { error: signUpErr } = await supabase.auth.signUp({ email: `example_${uniqueId}@email.com`, password: 'example-password' })
  if (signUpErr) console.warn(`auth.signUp returned error (continuing): ${signUpErr.message}`)

  const { error: createBucketErr } = await supabase.storage.createBucket(`avatars_${uniqueId}`)
  if (createBucketErr) console.warn(`storage.createBucket returned error (continuing): ${createBucketErr.message}`)

  const { error: deleteBucketErr } = await supabase.storage.deleteBucket(`avatars_${uniqueId}`)
  if (deleteBucketErr) console.warn(`storage.deleteBucket returned error (continuing): ${deleteBucketErr.message}`)

  await supabase.functions.invoke(`function_${uniqueId}`)
  await supabase.functions.invoke('hello')

  // Realtime: the "Billing metrics" log we assert on is emitted by realtime's
  // PromEx tenant poller (every ~5s) but ONLY while a tenant has a live
  // connection. A short-lived 2s subscription almost never overlaps a poll
  // tick, so it produced zero emissions and the realtime_logs wait below timed
  // out. Keep the channel subscribed through the ingestion polling instead, so
  // emissions occur continuously; it's torn down after Promise.all resolves.
  const channel = supabase.channel(`test_${uniqueId}`)
  channel.subscribe()
  await new Promise(r => setTimeout(r, 2000))

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

  // Wait for ingestion of test setup events in parallel before the test bodies
  // run their searches. Replaces a fixed 30s sleep that was racy on slow CI
  // runners.
  //
  // - edge_logs / auth_logs / storage_logs poll for uniqueId-tagged events from
  //   the setup above.
  // - realtime_logs polls for "Billing" emissions, produced every ~5s while the
  //   channel opened above stays subscribed.
  // - postgrest_logs polls for the periodic "Config reloaded" infra emission.
  //   The corresponding test bodies assert on these strings with a 15s expect
  //   timeout, so without this wait the tests race the gap between emissions.
  //
  // edge functions /home/deno/functions/hello and the cron job reliably hit
  // historical entries via the test bodies' date-windowed searches, so we
  // don't poll those here.
  await Promise.all([
    waitForLogs(request, 'edge_logs', `function_${uniqueId}`),
    waitForLogs(request, 'auth_logs', `example_${uniqueId}`),
    waitForLogs(request, 'storage_logs', `avatars_${uniqueId}`),
    waitForLogs(request, 'realtime_logs', 'Billing'),
    waitForLogs(request, 'postgrest_logs', 'Config reloaded'),
  ]);

  await supabase.removeChannel(channel)
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
