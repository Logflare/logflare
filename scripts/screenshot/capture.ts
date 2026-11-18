/**
 * Saves a screenshot with a manifest of plain-English expectations.
 *
 * The spec body proves the DOM state with Playwright `expect` before it calls
 * `capture`. The `expectations` are claims about the picture instead: colors,
 * layout, icons, copy. They are not executed. They are written to
 * `.generated/<name>.json` next to `.generated/<name>.png`, so the reviewer
 * reads the PNG and confirms or refutes each claim.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "@playwright/test";

export const GENERATED_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), ".generated");

const MAX_EXPECTATIONS = 3;

export type CaptureOptions = {
  /** File name without extension. Use `<spec-slug>-<NN>-<what-it-shows>`. */
  name: string;
  /** One to three visual claims that a reviewer checks against the PNG. */
  expectations: string[];
  /** Capture only this element instead of the viewport. */
  clipSelector?: string;
  /** Hover this element first, so `:hover` styles show. */
  hoverSelector?: string;
  /** Capture the full scrollable page instead of the viewport. */
  fullPage?: boolean;
};

export async function capture(page: Page, options: CaptureOptions): Promise<string> {
  const { name, expectations, clipSelector, hoverSelector, fullPage = false } = options;

  if (expectations.length === 0 || expectations.length > MAX_EXPECTATIONS) {
    throw new Error(
      `capture("${name}") needs 1 to ${MAX_EXPECTATIONS} expectations, got ${expectations.length}. ` +
        "If a screenshot needs more claims, it shows more than one thing: take a second capture.",
    );
  }

  fs.mkdirSync(GENERATED_DIR, { recursive: true });
  const pngPath = path.join(GENERATED_DIR, `${name}.png`);

  if (hoverSelector) await page.locator(hoverSelector).first().hover();
  if (clipSelector) {
    await page.locator(clipSelector).first().screenshot({ path: pngPath });
  } else {
    await page.screenshot({ path: pngPath, fullPage });
  }

  fs.writeFileSync(
    path.join(GENERATED_DIR, `${name}.json`),
    JSON.stringify(
      { name, url: page.url(), capturedAt: new Date().toISOString(), expectations },
      null,
      2,
    ),
  );

  return pngPath;
}
