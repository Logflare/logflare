// Loads the UI in Chromium and fails if a static asset does not load.
// Usage: NODE_PATH="$(npm root -g)" node check-assets.cjs [base-url] [screenshot-path]
const { chromium } = require("playwright");

const ASSET_PATH = /^\/(js|css|images|fonts|favicon|manifest|robots|worker)/;

async function main() {
  const base = process.argv[2] ?? "http://localhost:4000";
  const shot = process.argv[3] ?? "ui-qa.png";
  const origin = new URL(base).origin;

  const browser = await chromium.launch();
  const page = await browser.newPage();
  const assets = new Map();
  const failures = [];

  page.on("response", (res) => {
    const url = new URL(res.url());
    if (url.origin === origin && ASSET_PATH.test(url.pathname)) {
      assets.set(url.pathname + url.search, res.status());
    }
  });
  page.on("requestfailed", (req) => failures.push(`request failed: ${req.url()} ${req.failure()?.errorText}`));
  page.on("console", (msg) => msg.type() === "error" && failures.push(`console: ${msg.text()}`));

  const res = await page.goto(`${base}/`, { waitUntil: "networkidle" });
  console.log(`page ${page.url()} -> ${res.status()}`);

  const refs = await page.evaluate(() =>
    [...document.querySelectorAll("link[href], script[src], img[src]")].map((e) => e.href || e.src),
  );
  for (const ref of new Set(refs)) {
    const url = new URL(ref);
    const key = url.pathname + url.search;
    if (url.origin !== origin || !ASSET_PATH.test(url.pathname) || assets.has(key)) continue;
    assets.set(key, (await page.request.get(ref)).status());
  }

  const styled = await page.evaluate(
    () =>
      [...document.styleSheets].filter((s) => s.href && new URL(s.href).origin === location.origin && s.cssRules.length > 0)
        .length,
  );
  const brokenImages = await page.evaluate(() =>
    [...document.images].filter((i) => i.complete && i.naturalWidth === 0).map((i) => i.src),
  );

  for (const [path, status] of assets) console.log(`${status} ${path}`);
  console.log(`same-origin stylesheets with rules: ${styled}`);
  console.log(`broken images: ${JSON.stringify(brokenImages)}`);
  console.log(`failures: ${JSON.stringify(failures)}`);

  await page.screenshot({ path: shot, fullPage: true });
  await browser.close();

  const bad = [...assets.values()].filter((status) => status >= 400);
  const ok = res.ok() && assets.size > 0 && bad.length === 0 && styled > 0 && brokenImages.length === 0;
  console.log(ok ? "PASS" : "FAIL");
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
