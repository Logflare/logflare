import { expect, test } from '@playwright/test';

import { forceIpv4Loopback } from '../lib/env';

test.describe('forceIpv4Loopback', () => {
  test('rewrites localhost while preserving the port, path, query and fragment', () => {
    expect(forceIpv4Loopback('http://localhost:8000/api/logs?source=test#latest')).toBe(
      'http://127.0.0.1:8000/api/logs?source=test#latest',
    );
  });

  test('rewrites an explicit IPv6 loopback address', () => {
    expect(forceIpv4Loopback('http://[::1]:8000/')).toBe('http://127.0.0.1:8000');
  });

  test('leaves remote and already-IPv4 URLs unchanged', () => {
    expect(forceIpv4Loopback('https://example.com/api/')).toBe('https://example.com/api/');
    expect(forceIpv4Loopback('http://127.0.0.1:8000/')).toBe('http://127.0.0.1:8000/');
  });

  test('leaves invalid URLs unchanged', () => {
    expect(forceIpv4Loopback('not a URL')).toBe('not a URL');
  });
});
