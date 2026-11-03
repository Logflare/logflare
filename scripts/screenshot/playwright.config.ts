import { defineConfig } from "@playwright/test";

// Specs run against a server that is already up: a release image or `mix phx.server`.
// See .claude/skills/ui-qa/SKILL.md.
export default defineConfig({
  testDir: "./specs",
  outputDir: "./test-results",
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  reporter: [["list"]],
  use: {
    baseURL: process.env.LOGFLARE_URL ?? "http://localhost:4000",
    viewport: { width: 1280, height: 800 },
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_PATH
      ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_PATH }
      : {},
  },
});
