import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { spawn } from 'child_process';
import { mkdir, rm, writeFile, readFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { Readable } from 'stream';
import { supabase } from '@worker/config/supabase';
import { env } from '@worker/config/env';
import { logger } from '@worker/config/logger';

const POLL_INTERVAL_MS = 2000;
const PROCESSING_TIMEOUT_MS = 600_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface JobOptions {
  crf?: number;
  maxEdge?: number;
}

interface SupabaseJob {
  id: string;
  input_key: string;
  options: JobOptions | null;
}

// ---------------------------------------------------------------------------
// FFmpeg helpers (mirrors the pattern in queue/video/processor.ts)
// ---------------------------------------------------------------------------

type FfmpegRunResult = {
  code: number | null;
  signal: NodeJS.Signals | null;
  stderr: string;
  timedOut: boolean;
};

function runFfmpeg(args: string[], timeoutMs: number): Promise<FfmpegRunResult> {
  return new Promise((resolve, reject) => {
    const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    const stderrChunks: Buffer[] = [];
    let stderrLen = 0;
    const MAX_STDERR = 256 * 1024;
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      setTimeout(() => child.kill('SIGKILL'), 5000).unref();
    }, timeoutMs);

    child.stderr?.on('data', (chunk: Buffer) => {
      stderrLen += chunk.length;
      if (stderrLen <= MAX_STDERR) {
        stderrChunks.push(chunk);
      }
    });

    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timer);
      resolve({
        code,
        signal,
        stderr: Buffer.concat(stderrChunks).toString('utf8'),
        timedOut
      });
    });
  });
}

function formatFfmpegError(result: FfmpegRunResult): string {
  const tail = result.stderr.trim().split('\n').slice(-20).join('\n');

  if (result.timedOut) {
    return `Encoding timed out after ${Math.round(PROCESSING_TIMEOUT_MS / 1000)}s`;
  }

  if (result.signal === 'SIGKILL' && !tail) {
    return 'The encoder was killed by the OS (likely out of memory)';
  }

  if (result.signal) {
    return `ffmpeg killed by ${result.signal}${tail ? `. Last output:\n${tail}` : ''}`;
  }

  return `ffmpeg exited with code ${result.code ?? 'unknown'}${tail ? `. Last output:\n${tail}` : ''}`;
}

// ---------------------------------------------------------------------------
// S3 helpers
// ---------------------------------------------------------------------------

function buildS3Client(): S3Client {
  if (
    !env.S3_ENDPOINT ||
    !env.S3_REGION ||
    !env.S3_BUCKET ||
    !env.S3_ACCESS_KEY_ID ||
    !env.S3_SECRET_ACCESS_KEY
  ) {
    throw new Error('S3 configuration is incomplete');
  }

  return new S3Client({
    endpoint: env.S3_ENDPOINT,
    region: env.S3_REGION,
    forcePathStyle: true,
    credentials: {
      accessKeyId: env.S3_ACCESS_KEY_ID,
      secretAccessKey: env.S3_SECRET_ACCESS_KEY
    }
  });
}

async function downloadFromS3(s3: S3Client, key: string, destPath: string): Promise<void> {
  const response = await s3.send(
    new GetObjectCommand({ Bucket: env.S3_BUCKET!, Key: key })
  );

  if (!response.Body) {
    throw new Error(`S3 returned empty body for key: ${key}`);
  }

  const chunks: Buffer[] = [];
  for await (const chunk of response.Body as Readable) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }

  await writeFile(destPath, Buffer.concat(chunks));
}

async function uploadOutputToS3(s3: S3Client, filePath: string, outputKey: string): Promise<string> {
  const fileBuffer = await readFile(filePath);

  await s3.send(
    new PutObjectCommand({
      Bucket: env.S3_BUCKET!,
      Key: outputKey,
      Body: fileBuffer,
      ContentType: 'video/mp4',
      ACL: 'public-read'
    })
  );

  const url = env.S3_PUBLIC_URL
    ? `${env.S3_PUBLIC_URL}/${outputKey}`
    : `${env.S3_ENDPOINT}/${env.S3_BUCKET}/${outputKey}`;

  return url;
}

// ---------------------------------------------------------------------------
// Job processing
// ---------------------------------------------------------------------------

async function processJob(job: SupabaseJob): Promise<void> {
  const { id: jobId, input_key: inputKey, options } = job;
  const crf = options?.crf ?? 23;
  const maxEdge = options?.maxEdge && options.maxEdge > 0 ? options.maxEdge : 1920;

  logger.info({ jobId, inputKey, crf, maxEdge }, 'Supabase poller: processing job');

  const workDir = join(tmpdir(), `squeezey-${jobId}`);
  const inputPath = join(workDir, 'input');
  const outputPath = join(workDir, 'output.mp4');
  const outputKey = `ffmpeg-rest/outputs/${jobId}.mp4`;

  await mkdir(workDir, { recursive: true });

  try {
    const s3 = buildS3Client();

    // Download input
    logger.info({ jobId, inputKey }, 'Supabase poller: downloading input from S3');
    await downloadFromS3(s3, inputKey, inputPath);

    // Run FFmpeg
    logger.info({ jobId }, 'Supabase poller: starting FFmpeg encode');
    const result = await runFfmpeg(
      [
        '-loglevel', 'warning',
        '-threads', '1',
        '-i', inputPath,
        '-map', '0:v:0',
        '-map', '0:a:0?',
        '-dn',
        '-sn',
        '-ignore_unknown',
        '-vf', `scale='if(gte(iw,ih),min(${maxEdge},iw),-2)':'if(gte(iw,ih),-2,min(${maxEdge},ih))'`,
        '-pix_fmt', 'yuv420p',
        '-codec:v', 'libx264',
        '-preset', 'medium',
        '-crf', crf.toString(),
        '-codec:a', 'aac',
        '-b:a', '128k',
        '-max_muxing_queue_size', '1024',
        '-movflags', '+faststart',
        '-y', outputPath
      ],
      PROCESSING_TIMEOUT_MS
    );

    if (result.code !== 0 || result.signal) {
      throw new Error(formatFfmpegError(result));
    }

    // Upload output
    logger.info({ jobId, outputKey }, 'Supabase poller: uploading output to S3');
    await uploadOutputToS3(s3, outputPath, outputKey);

    // Mark done
    await supabase!
      .from('jobs')
      .update({
        status: 'done',
        output_key: outputKey,
        finished_at: new Date().toISOString()
      })
      .eq('id', jobId);

    logger.info({ jobId, outputKey }, 'Supabase poller: job completed successfully');
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error({ jobId, error: errorMessage }, 'Supabase poller: job failed');

    await supabase!
      .from('jobs')
      .update({
        status: 'failed',
        error: errorMessage,
        finished_at: new Date().toISOString()
      })
      .eq('id', jobId);
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// Poll loop
// ---------------------------------------------------------------------------

async function pollOnce(): Promise<void> {
  const { data, error } = await supabase!.rpc('claim_next_job');

  if (error) {
    logger.error({ error: error.message }, 'Supabase poller: claim_next_job RPC failed');
    return;
  }

  const job = (Array.isArray(data) ? data[0] : data) as SupabaseJob | undefined;
  if (!job || !job.id) {
    // No pending jobs — nothing to do this cycle
    return;
  }

  logger.info({ jobId: job.id }, 'Supabase poller: claimed job');

  await processJob(job);
}

export function startSupabasePoller(): void {
  if (!supabase) {
    // Credentials not configured — poller is disabled
    return;
  }

  logger.info('Supabase poller: starting (poll interval 2s)');

  const loop = async (): Promise<void> => {
    try {
      await pollOnce();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error({ error: msg }, 'Supabase poller: unexpected error in poll loop');
    }

    setTimeout(() => void loop(), POLL_INTERVAL_MS);
  };

  // Kick off without blocking the caller
  setTimeout(() => void loop(), 0);
}
