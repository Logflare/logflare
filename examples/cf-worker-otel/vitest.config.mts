import { defineConfig } from 'vitest/config';
import { cloudflareTest } from '@cloudflare/vitest-plugin';

export default defineConfig({
	plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.json' } })],
	test: { testTimeout: 60_000 },
});
