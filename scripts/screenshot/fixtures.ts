import { test as base, expect } from "@playwright/test";

const STATIC_PATH = /^\/(js|css|images|fonts|favicon|manifest|robots|worker)/;

/**
 * Every spec fails if a same-origin static asset returns an error, a request
 * fails, or the page logs a console error. A screenshot of a page with a
 * missing stylesheet must not pass.
 */
export const test = base.extend<{ assetGuard: void }>({
  assetGuard: [
    async ({ page, baseURL }, use) => {
      const origin = new URL(baseURL ?? "http://localhost:4000").origin;
      const problems: string[] = [];

      page.on("response", (res) => {
        const url = new URL(res.url());
        if (url.origin === origin && STATIC_PATH.test(url.pathname) && res.status() >= 400) {
          problems.push(`${res.status()} ${url.pathname}${url.search}`);
        }
      });
      page.on("requestfailed", (req) => {
        if (new URL(req.url()).origin === origin) {
          problems.push(`request failed: ${req.url()} ${req.failure()?.errorText}`);
        }
      });
      page.on("console", (msg) => {
        if (msg.type() === "error") problems.push(`console error: ${msg.text()}`);
      });

      await use();

      expect(problems, "static assets, requests, and console").toEqual([]);
    },
    { auto: true },
  ],
});

export { expect };
