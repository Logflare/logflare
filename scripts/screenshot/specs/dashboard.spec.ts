import { capture } from "../capture";
import { expect, test } from "../fixtures";

test("dashboard loads with styles, icons, and images", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/dashboard/);
  await expect(page.getByText("~/logs", { exact: true })).toBeVisible();
  await expect(page.getByText("New source")).toBeVisible();

  const styledSheets = await page.evaluate(
    () =>
      [...document.styleSheets].filter(
        (s) => s.href && new URL(s.href).origin === location.origin && s.cssRules.length > 0,
      ).length,
  );
  expect(styledSheets).toBeGreaterThan(0);

  const brokenImages = await page.evaluate(() =>
    [...document.images].filter((i) => i.complete && i.naturalWidth === 0).map((i) => i.src),
  );
  expect(brokenImages).toEqual([]);

  await capture(page, {
    name: "dashboard-01-page",
    expectations: [
      "The top bar is green and shows the Logflare logo and version on the left.",
      "The content area has a dark background with Members, sources, and Integrations columns.",
      "Run a query and New source show as blue buttons, not as plain links.",
    ],
  });

  await capture(page, {
    name: "dashboard-02-subhead-icons",
    clipSelector: ".subhead",
    expectations: [
      "Each subhead link (ingest API key, access tokens, billing, help) has an icon to its left.",
      "The icons are glyphs, not empty boxes or missing-character squares.",
    ],
  });
});
