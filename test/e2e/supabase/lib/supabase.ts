import { createClient } from '@supabase/supabase-js'

import { supabasePublicUrl } from './env';

const serviceRoleKey = process.env.SERVICE_ROLE_KEY;

if (!supabasePublicUrl || !serviceRoleKey) {
	throw new Error('Missing SUPABASE_PUBLIC_URL or SERVICE_ROLE_KEY in supabase/docker/.env');
}

export default createClient(supabasePublicUrl, serviceRoleKey);
