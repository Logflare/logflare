import dotenv from 'dotenv';
import path from 'path';

dotenv.config({
  path: path.resolve(__dirname, '..', 'supabase', 'docker', '.env'),
});

// Node 18+ honours the OS resolver order for `localhost`, which on the GitHub
// Actions runners returns ::1 first. The Supabase stack publishes its ports on
// IPv4 only, so anything that dials `http://localhost:8000` fails with
// `connect ECONNREFUSED ::1:8000` (Playwright's request context) or a bare
// `fetch failed` (supabase-js). Pin the loopback host to IPv4.
export function forceIpv4Loopback(url: string): string {
  try {
    const parsed = new URL(url);

    if (parsed.hostname === 'localhost' || parsed.hostname === '::1') {
      parsed.hostname = '127.0.0.1';
    }

    return parsed.toString().replace(/\/$/, '');
  } catch {
    return url;
  }
}

export const supabasePublicUrl = process.env.SUPABASE_PUBLIC_URL
  ? forceIpv4Loopback(process.env.SUPABASE_PUBLIC_URL)
  : undefined;
