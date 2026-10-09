import { capture } from "../capture";
import { expect, test } from "../fixtures";

// Run by .claude/skills/ingest-qa/scripts/run.sh after it ingests a batch tagged with a run id.
const sourceId = process.env.INGEST_QA_SOURCE_ID;
const runId = process.env.INGEST_QA_RUN_ID ?? "";
const messages = (process.env.INGEST_QA_MESSAGES ?? "").split("\n").filter(Boolean);

test("ingested events show in source search", async ({ page }) => {
  test.skip(!sourceId || !runId || messages.length === 0, "Run through the ingest-qa skill.");

  // Sign in first: the single-tenant sign-in redirect drops the query string of the first URL.
  await page.goto("/dashboard");
  await page.goto(`/sources/${sourceId}/search?querystring=${encodeURIComponent(runId)}`);
  await expect(page).toHaveURL(new RegExp(`querystring=[^&]*${runId}`));

  const results = page.locator("#logs-list > li[data-event-id]");
  for (const message of messages) {
    await expect(results.filter({ hasText: message }).first()).toBeVisible({ timeout: 30_000 });
  }
  await expect(results).toHaveCount(messages.length);

  await expect(page.locator(".monaco-editor .view-lines")).toContainText(runId);

  await capture(page, {
    name: "ingest-search-01-query",
    expectations: [
      `The query editor above the Search button contains ${runId}.`,
      "The subhead reads ~/logs/qa_ingest_main/search.",
    ],
  });

  // The page scrolls to the newest event while tailing, which puts the first rows under the sticky header.
  await page.evaluate(() => window.scrollTo(0, 0));

  await capture(page, {
    name: "ingest-search-02-results",
    clipSelector: "#logs-list",
    expectations: [
      `The list has ${messages.length} rows, and each message ends with ${runId}.`,
      "Rows come from http_token, http_name, websocket and grpc, each with error, warn and info.",
      "Each row starts with a green timestamp and ends with a view context link.",
    ],
  });
});
