import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      include: ["js/monaco_hook.js"],
      reporter: ["text"],
      thresholds: {
        lines: 90,
        functions: 90,
        statements: 90,
      },
    },
  },
});
