import { createClient } from '@supabase/supabase-js'

import dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.resolve(__dirname, 'supabase/docker/.env') });

const publicUrl = process.env.SUPABASE_PUBLIC_URL;
const serviceRoleKey = process.env.SERVICE_ROLE_KEY;

if (!publicUrl || !serviceRoleKey) {
	throw new Error('Missing SUPABASE_PUBLIC_URL or SERVICE_ROLE_KEY in supabase/docker/.env');
}

export default createClient(publicUrl, serviceRoleKey);