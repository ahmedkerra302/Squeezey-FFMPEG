import { createClient } from '@supabase/supabase-js';
import { env } from './env';
import { logger } from './logger';

const supabaseUrl = env.SUPABASE_URL;
const supabaseKey = env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  logger.warn('Supabase credentials not configured; database poller will not run');
}

export const supabase =
  supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;
