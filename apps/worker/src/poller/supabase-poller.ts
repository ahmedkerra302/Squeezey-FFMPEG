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
  outputFormat?: string;
  quality?: 'off' | 'high' | 'balanced' | 'smallest';
}

interface SupabaseJob {
  id: string;
  input_key: string;
  options: JobOptions | null;
}

// ---------------------------------------------------------------------------
// FFmpeg helpers
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

// Probe input file for video + audio codec names via ffprobe.
type ProbeResult = {
  videoCodec: string | null;
  audioCodec: string | null;
  pixFmt: string | null;
  colorPrimaries: string | null;
  colorTransfer: string | null;
  colorSpace: string | null;
};

function probeCodecs(inputPath: string): Promise<ProbeResult> {
  return new Promise((resolve) => {
    const args = [
      '-v', 'error',
      '-print_format', 'json',
      '-show_streams',
      '-select_streams', 'v:0',
      inputPath
    ];
    const v = spawn('ffprobe', args, { stdio: ['ignore', 'pipe', 'ignore'] });
    const chunks: Buffer[] = [];
    v.stdout?.on('data', (c: Buffer) => chunks.push(c));
    v.on('close', () => {
      let videoCodec: string | null = null;
      let pixFmt: string | null = null;
      let colorPrimaries: string | null = null;
      let colorTransfer: string | null = null;
      let colorSpace: string | null = null;
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        const vs = parsed?.streams?.[0];
        videoCodec = vs?.codec_name ?? null;
        pixFmt = vs?.pix_fmt ?? null;
        colorPrimaries = vs?.color_primaries ?? null;
        colorTransfer = vs?.color_transfer ?? null;
        colorSpace = vs?.color_space ?? null;
      } catch { /* ignore */ }

      const args2 = [
        '-v', 'error',
        '-print_format', 'json',
        '-show_streams',
        '-select_streams', 'a:0',
        inputPath
      ];
      const a = spawn('ffprobe', args2, { stdio: ['ignore', 'pipe', 'ignore'] });
      const aChunks: Buffer[] = [];
      a.stdout?.on('data', (c: Buffer) => aChunks.push(c));
      a.on('close', () => {
        let audioCodec: string | null = null;
        try {
          const parsed = JSON.parse(Buffer.concat(aChunks).toString('utf8'));
          audioCodec = parsed?.streams?.[0]?.codec_name ?? null;
        } catch { /* ignore */ }
        resolve({ videoCodec, audioCodec, pixFmt, colorPrimaries, colorTransfer, colorSpace });
      });
      a.on('error', () => resolve({ videoCodec, audioCodec: null, pixFmt, colorPrimaries, colorTransfer, colorSpace }));
    });
    v.on('error', () => resolve({ videoCodec: null, audioCodec: null, pixFmt: null, colorPrimaries: null, colorTransfer: null, colorSpace: null }));
  });
}

// ---------------------------------------------------------------------------
// Output format + container-compatibility matrix
// ---------------------------------------------------------------------------

type OutputFormatConfig = {
  ext: string;
  mime: string;
  vcodec: string;          // re-encode target video codec
  acodec: string;          // re-encode target audio codec
  defaultCrf: number;      // sane default CRF when "Off" forces a re-encode
  extra: string[];
  videoCopyOk: Set<string>; // input video codecs that can be remuxed into this container
  audioCopyOk: Set<string>; // input audio codecs that can be remuxed into this container
};

const FORMAT_MAP: Record<string, OutputFormatConfig> = {
  mp4: {
    ext: 'mp4', mime: 'video/mp4',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 23,
    extra: ['-movflags', '+faststart'],
    videoCopyOk: new Set(['h264', 'hevc', 'mpeg4', 'av1']),
    audioCopyOk: new Set(['aac', 'mp3', 'ac3'])
  },
  mov: {
    ext: 'mov', mime: 'video/quicktime',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 23,
    extra: ['-movflags', '+faststart'],
    videoCopyOk: new Set(['h264', 'hevc', 'mpeg4', 'prores', 'av1']),
    audioCopyOk: new Set(['aac', 'mp3', 'pcm_s16le', 'pcm_s24le', 'ac3'])
  },
  m4v: {
    ext: 'm4v', mime: 'video/x-m4v',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 23,
    extra: ['-movflags', '+faststart'],
    videoCopyOk: new Set(['h264', 'hevc']),
    audioCopyOk: new Set(['aac', 'mp3'])
  },
  mkv: {
    ext: 'mkv', mime: 'video/x-matroska',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 23,
    extra: [],
    // MKV swallows almost anything
    videoCopyOk: new Set(['h264', 'hevc', 'mpeg4', 'vp8', 'vp9', 'av1', 'theora', 'mpeg2video']),
    audioCopyOk: new Set(['aac', 'mp3', 'opus', 'vorbis', 'ac3', 'flac', 'pcm_s16le', 'pcm_s24le'])
  },
  webm: {
    ext: 'webm', mime: 'video/webm',
    vcodec: 'libvpx-vp9', acodec: 'libopus', defaultCrf: 32,
    extra: ['-row-mt', '1'],
    videoCopyOk: new Set(['vp8', 'vp9', 'av1']),
    audioCopyOk: new Set(['opus', 'vorbis'])
  },
  avi: {
    ext: 'avi', mime: 'video/x-msvideo',
    vcodec: 'mpeg4', acodec: 'libmp3lame', defaultCrf: 5, // mpeg4 uses -q:v
    extra: [],
    videoCopyOk: new Set(['mpeg4', 'h264', 'mjpeg']),
    audioCopyOk: new Set(['mp3', 'ac3', 'pcm_s16le'])
  },
  '3gp': {
    ext: '3gp', mime: 'video/3gpp',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 28,
    extra: ['-profile:v', 'baseline', '-level', '3.0'],
    videoCopyOk: new Set(['h264', 'h263', 'mpeg4']),
    audioCopyOk: new Set(['aac', 'amr_nb'])
  },
  ts: {
    ext: 'ts', mime: 'video/mp2t',
    vcodec: 'libx264', acodec: 'aac', defaultCrf: 23,
    extra: ['-f', 'mpegts'],
    videoCopyOk: new Set(['h264', 'hevc', 'mpeg2video']),
    audioCopyOk: new Set(['aac', 'mp3', 'ac3'])
  }
};

function resolveFormat(name?: string): OutputFormatConfig {
  return FORMAT_MAP[(name || 'mp4').toLowerCase()] || FORMAT_MAP.mp4;
}

// Per-encoder quality tier table. Mirrors FreeConvert-style defaults.
// libx264/libx265/libvpx-vp9 use CRF; mpeg4 uses -q:v (1..31, lower=better).
type Quality = 'off' | 'high' | 'balanced' | 'smallest';
const QUALITY_TABLE: Record<string, Record<Quality, number>> = {
  libx264:     { off: 18, high: 20, balanced: 23, smallest: 28 },
  libx265:     { off: 22, high: 24, balanced: 28, smallest: 32 },
  'libvpx-vp9':{ off: 30, high: 32, balanced: 35, smallest: 38 },
  mpeg4:       { off:  2, high:  3, balanced:  5, smallest:  8 }
};

function qualityFromOptions(opts: { quality?: string; crf?: number }): Quality {
  const q = (opts.quality ?? '').toLowerCase();
  if (q === 'off' || q === 'high' || q === 'balanced' || q === 'smallest') return q as Quality;
  // Legacy fallback: derive from crf number.
  const c = Number(opts.crf ?? 23);
  if (!c || c === 0) return 'off';
  if (c <= 22) return 'high';
  if (c <= 28) return 'balanced';
  return 'smallest';
}

function encoderValue(encoder: string, quality: Quality, fallback: number): number {
  return QUALITY_TABLE[encoder]?.[quality] ?? fallback;
}

// Build ffmpeg args with codec-aware copy/re-encode decisions.
function buildVideoEncodeArgs(
  inputPath: string,
  outputPath: string,
  quality: Quality,
  maxEdge: number,
  format: OutputFormatConfig,
  probe: ProbeResult
): { args: string[]; mode: 'copy' | 'reencode-default' | 'reencode' } {
  const baseArgs = [
    '-loglevel', 'warning',
    '-threads', '1',
    '-i', inputPath,
    '-map', '0:v:0',
    '-map', '0:a:0?',
    '-dn',
    '-sn',
    '-ignore_unknown'
  ];

  const videoCompatible = probe.videoCodec ? format.videoCopyOk.has(probe.videoCodec) : false;
  const audioCompatible = !probe.audioCodec || format.audioCopyOk.has(probe.audioCodec);

  // "Off" => try true remux. Only safe when both streams are container-compatible.
  if (quality === 'off' && videoCompatible && audioCompatible) {
    return {
      mode: 'copy',
      args: [...baseArgs, '-c', 'copy', ...format.extra, '-y', outputPath]
    };
  }

  // Forced re-encode. "Off" still maps to a visually-lossless tier per codec
  // (mirrors FreeConvert behavior: silent re-encode at high quality).
  const mode = quality === 'off' ? 'reencode-default' : 'reencode';
  return {
    mode,
    args: buildReencodeArgs(baseArgs, outputPath, quality, maxEdge, format, probe, videoCompatible, audioCompatible)
  };
}

function buildReencodeArgs(
  baseArgs: string[],
  outputPath: string,
  quality: Quality,
  maxEdge: number,
  format: OutputFormatConfig,
  probe: ProbeResult,
  videoCompatible: boolean,
  audioCompatible: boolean
): string[] {
  const args = [...baseArgs];

  if (videoCompatible) {
    args.push('-c:v', 'copy');
  } else {
    // For "Off" / "High" we keep original resolution (visually lossless intent).
    const keepOriginal = quality === 'off' || quality === 'high';
    if (!keepOriginal) {
      args.push(
        '-vf',
        `scale='if(gte(iw,ih),min(${maxEdge},iw),-2)':'if(gte(iw,ih),-2,min(${maxEdge},ih))'`
      );
    }

    // 10-bit + HDR passthrough: only codecs that actually support it.
    const is10bit = (probe.pixFmt ?? '').includes('10');
    let pixFmt = 'yuv420p';
    if (is10bit && format.vcodec === 'libvpx-vp9') pixFmt = 'yuv420p10le';
    else if (is10bit && format.vcodec === 'libx265') pixFmt = 'yuv420p10le';
    args.push('-pix_fmt', pixFmt);

    args.push('-codec:v', format.vcodec);

    if (format.vcodec === 'libx264') {
      const crf = encoderValue('libx264', quality, format.defaultCrf);
      args.push('-preset', 'medium', '-crf', String(crf));
    } else if (format.vcodec === 'libx265') {
      const crf = encoderValue('libx265', quality, format.defaultCrf);
      args.push('-preset', 'medium', '-crf', String(crf));
      if (pixFmt === 'yuv420p10le') args.push('-profile:v', 'main10');
    } else if (format.vcodec === 'libvpx-vp9') {
      const crf = encoderValue('libvpx-vp9', quality, format.defaultCrf);
      args.push(
        '-b:v', '0',
        '-crf', String(crf),
        '-row-mt', '1',
        '-tile-columns', '2',
        '-deadline', 'good',
        '-cpu-used', '4'
      );
      if (pixFmt === 'yuv420p10le') args.push('-profile:v', '2');
    } else if (format.vcodec === 'mpeg4') {
      const qv = encoderValue('mpeg4', quality, 5);
      args.push('-q:v', String(qv));
    } else {
      const crf = encoderValue(format.vcodec, quality, format.defaultCrf);
      args.push('-crf', String(crf));
    }

    // HDR color metadata passthrough (only meaningful for codecs that store it).
    if (
      (format.vcodec === 'libvpx-vp9' || format.vcodec === 'libx265') &&
      probe.colorPrimaries && probe.colorTransfer && probe.colorSpace
    ) {
      args.push(
        '-color_primaries', probe.colorPrimaries,
        '-color_trc', probe.colorTransfer,
        '-colorspace', probe.colorSpace
      );
    }
  }

  if (probe.audioCodec && audioCompatible) {
    args.push('-c:a', 'copy');
  } else {
    args.push('-codec:a', format.acodec, '-b:a', '128k');
  }

  args.push('-max_muxing_queue_size', '1024', ...format.extra, '-y', outputPath);
  return args;
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

async function uploadOutputToS3(s3: S3Client, filePath: string, outputKey: string, contentType: string): Promise<string> {
  const fileBuffer = await readFile(filePath);

  await s3.send(
    new PutObjectCommand({
      Bucket: env.S3_BUCKET!,
      Key: outputKey,
      Body: fileBuffer,
      ContentType: contentType,
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
  const quality = qualityFromOptions(options ?? {});
  const maxEdge = options?.maxEdge && options.maxEdge > 0 ? options.maxEdge : 1920;
  const outputFormat = resolveFormat(options?.outputFormat);

  logger.info({ jobId, inputKey, quality, maxEdge, outputFormat: outputFormat.ext }, 'Supabase poller: processing job');

  const workDir = join(tmpdir(), `squeezey-${jobId}`);
  const inputPath = join(workDir, 'input');
  const outputPath = join(workDir, `output.${outputFormat.ext}`);
  const outputKey = `ffmpeg-rest/outputs/${jobId}.${outputFormat.ext}`;

  await mkdir(workDir, { recursive: true });

  try {
    const s3 = buildS3Client();

    logger.info({ jobId, inputKey }, 'Supabase poller: downloading input from S3');
    await downloadFromS3(s3, inputKey, inputPath);

    const probe = await probeCodecs(inputPath);
    logger.info({ jobId, probe }, 'Supabase poller: probed input codecs');

    const { args, mode } = buildVideoEncodeArgs(inputPath, outputPath, quality, maxEdge, outputFormat, probe);
    logger.info(
      { jobId, mode, target: outputFormat.ext, vcodec: outputFormat.vcodec, acodec: outputFormat.acodec },
      'Supabase poller: starting FFmpeg encode'
    );

    const result = await runFfmpeg(args, PROCESSING_TIMEOUT_MS);

    if (result.code !== 0 || result.signal) {
      throw new Error(formatFfmpegError(result));
    }

    logger.info({ jobId, outputKey }, 'Supabase poller: uploading output to S3');
    await uploadOutputToS3(s3, outputPath, outputKey, outputFormat.mime);

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
    return;
  }

  logger.info({ jobId: job.id }, 'Supabase poller: claimed job');

  await processJob(job);
}

export function startSupabasePoller(): void {
  if (!supabase) {
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

  setTimeout(() => void loop(), 0);
}
