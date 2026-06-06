import type { Job } from 'bullmq';
import type { JobResult } from '..';
import type {
  VideoToMp4JobData,
  VideoExtractAudioJobData,
  VideoExtractFramesJobData
} from '@shared/queue/video/schemas';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import { existsSync } from 'fs';
import { mkdir, rm } from 'fs/promises';
import { dirname, basename } from 'path';
import path from 'path';
import { uploadToS3 } from '@worker/utils/storage';

const execFileAsync = promisify(execFile);

const PROCESSING_TIMEOUT = 600000;

type FfmpegRunResult = {
  code: number | null;
  signal: NodeJS.Signals | null;
  stderr: string;
  timedOut: boolean;
};

// Run ffmpeg via spawn so we always capture stderr — even when the
// process is killed by signal (OOM = SIGKILL, timeout = SIGTERM).
// execFile drops stderr in those cases and we lose the real error.
function runFfmpeg(args: string[], timeoutMs: number): Promise<FfmpegRunResult> {
  return new Promise((resolve, reject) => {
    const child = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    const stderrChunks: Buffer[] = [];
    let stderrLen = 0;
    const MAX_STDERR = 256 * 1024; // 256 KB tail is plenty
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

function formatFfmpegError(prefix: string, result: FfmpegRunResult): string {
  const tail = result.stderr.trim().split('\n').slice(-20).join('\n');

  if (result.timedOut) {
    return `${prefix}: Encoding timed out after ${Math.round(PROCESSING_TIMEOUT / 1000)}s. The input is likely too large or too long for the current worker configuration.`;
  }

  if (result.signal === 'SIGKILL' && !tail) {
    return `${prefix}: The encoder was killed by the OS, most likely due to running out of memory. Increase the Railway service memory limit (currently likely 512MB) or use a smaller / shorter input.`;
  }

  if (result.signal) {
    return `${prefix}: ffmpeg killed by ${result.signal}${tail ? `. Last output:\n${tail}` : ' with no stderr output (likely an OS kill — out of memory or CPU limit).'}`;
  }

  return `${prefix}: ffmpeg exited with code ${result.code ?? 'unknown'}${tail ? `. Last output:\n${tail}` : ' (no stderr captured).'}`;
}

async function shouldCopyStreams(inputPath: string): Promise<boolean> {
  try {
    const { stdout: videoCodec } = await execFileAsync(
      'ffprobe',
      [
        '-v',
        'error',
        '-select_streams',
        'v:0',
        '-show_entries',
        'stream=codec_name',
        '-of',
        'default=noprint_wrappers=1:nokey=1',
        inputPath
      ],
      { timeout: 30000 }
    );

    const { stdout: audioCodec } = await execFileAsync(
      'ffprobe',
      [
        '-v',
        'error',
        '-select_streams',
        'a:0',
        '-show_entries',
        'stream=codec_name',
        '-of',
        'default=noprint_wrappers=1:nokey=1',
        inputPath
      ],
      { timeout: 30000 }
    );

    return videoCodec.trim() === 'h264' && audioCodec.trim() === 'aac';
  } catch {
    return false;
  }
}

export async function processVideoToMp4(job: Job<VideoToMp4JobData>): Promise<JobResult> {
  const { inputPath, outputPath, crf, preset, smartCopy } = job.data;

  if (!existsSync(inputPath)) {
    return {
      success: false,
      error: `Input file does not exist: ${inputPath}`
    };
  }

  try {
    const outputDir = dirname(outputPath);
    await mkdir(outputDir, { recursive: true });

    let result: FfmpegRunResult;

    if (smartCopy && (await shouldCopyStreams(inputPath))) {
      result = await runFfmpeg(
        ['-loglevel', 'warning', '-i', inputPath, '-c', 'copy', '-movflags', '+faststart', '-y', outputPath],
        PROCESSING_TIMEOUT
      );
    } else {
      result = await runFfmpeg(
        [
          '-loglevel', 'warning',
          '-threads', '1',
          '-i', inputPath,
          // Explicitly map only the first video + (optional) first audio
          // stream. This skips attached pictures / cover art (which trigger
          // "Unknown cover type" errors on iPhone & CapCut MP4s), subtitle
          // streams, and arbitrary data streams that can blow up memory.
          '-map', '0:v:0',
          '-map', '0:a:0?',
          '-dn',
          '-sn',
          '-ignore_unknown',
          '-vf', "scale='min(1280,iw)':'-2'",
          '-pix_fmt', 'yuv420p',
          '-codec:v', 'libx264',
          // Force the lowest-memory x264 settings. Railway Trial caps
          // the worker at ~512MB which is too tight for libx264's
          // default lookahead/reference buffers on 1080p inputs.
          // ultrafast + ref=1 + bframes=0 + no rc-lookahead keeps RSS
          // under ~250MB even on 67MB+ files.
          '-preset', 'ultrafast',
          '-tune', 'fastdecode,zerolatency',
          '-x264-params', 'ref=1:bframes=0:rc-lookahead=0:sliced-threads=0:sync-lookahead=0',
          '-crf', crf.toString(),
          '-codec:a', 'aac',
          '-b:a', '96k',
          '-ac', '2',
          '-max_muxing_queue_size', '1024',
          '-movflags', '+faststart',
          '-y', outputPath
        ],
        PROCESSING_TIMEOUT
      );
    }

    if (result.code !== 0 || result.signal) {
      return {
        success: false,
        error: formatFfmpegError('Failed to convert video to MP4', result)
      };
    }

    if (job.data.uploadToS3) {
      const { url } = await uploadToS3(outputPath, 'video/mp4', basename(outputPath));
      await rm(outputPath, { force: true });
      return {
        success: true,
        outputUrl: url
      };
    }

    return {
      success: true,
      outputPath
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      success: false,
      error: `Failed to convert video to MP4: ${errorMessage}`
    };
  }
}

export async function processVideoExtractAudio(job: Job<VideoExtractAudioJobData>): Promise<JobResult> {
  const { inputPath, outputPath, mono } = job.data;

  if (!existsSync(inputPath)) {
    return {
      success: false,
      error: `Input file does not exist: ${inputPath}`
    };
  }

  try {
    const outputDir = dirname(outputPath);
    await mkdir(outputDir, { recursive: true });

    const args = ['-loglevel', 'warning', '-i', inputPath, '-vn', '-acodec', 'pcm_s16le', '-ar', '44100'];

    if (mono) {
      args.push('-ac', '1');
    }

    args.push('-y', outputPath);

    const result = await runFfmpeg(args, PROCESSING_TIMEOUT);

    if (result.code !== 0 || result.signal) {
      return {
        success: false,
        error: formatFfmpegError('Failed to extract audio from video', result)
      };
    }

    if (job.data.uploadToS3) {
      const { url } = await uploadToS3(outputPath, 'audio/wav', basename(outputPath));
      await rm(outputPath, { force: true });
      return {
        success: true,
        outputUrl: url
      };
    }

    return {
      success: true,
      outputPath
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      success: false,
      error: `Failed to extract audio from video: ${errorMessage}`
    };
  }
}

export async function processVideoExtractFrames(job: Job<VideoExtractFramesJobData>): Promise<JobResult> {
  const { inputPath, outputDir, fps, format, quality, compress } = job.data;

  if (!existsSync(inputPath)) {
    return {
      success: false,
      error: `Input file does not exist: ${inputPath}`
    };
  }

  try {
    await mkdir(outputDir, { recursive: true });

    const ext = format === 'jpg' ? 'jpg' : 'png';
    const outputPattern = path.join(outputDir, `frame_%04d.${ext}`);

    const args = ['-loglevel', 'warning', '-i', inputPath, '-vf', `fps=${fps}`];

    if (format === 'jpg' && quality) {
      args.push('-q:v', quality.toString());
    }

    args.push('-y', outputPattern);

    const result = await runFfmpeg(args, PROCESSING_TIMEOUT);

    if (result.code !== 0 || result.signal) {
      return {
        success: false,
        error: formatFfmpegError('Failed to extract frames from video', result)
      };
    }

    const { readdirSync } = await import('fs');
    const frames = readdirSync(outputDir)
      .filter((f) => f.endsWith(`.${ext}`))
      .map((f) => path.join(outputDir, f));

    if (frames.length === 0) {
      return {
        success: false,
        error: 'No frames were extracted from the video'
      };
    }

    if (compress === 'zip') {
      const { default: archiver } = await import('archiver');
      const { createWriteStream } = await import('fs');
      const archivePath = `${outputDir}.zip`;
      const output = createWriteStream(archivePath);
      const archive = archiver('zip', { zlib: { level: 9 } });

      archive.pipe(output);
      archive.directory(outputDir, false);
      await archive.finalize();

      await new Promise<void>((resolve, reject) => {
        output.on('close', () => resolve());
        output.on('error', reject);
      });

      if (job.data.uploadToS3) {
        const { url } = await uploadToS3(archivePath, 'application/zip', basename(archivePath));
        await rm(dirname(outputDir), { recursive: true, force: true });
        return {
          success: true,
          outputUrl: url
        };
      }

      return {
        success: true,
        outputPath: archivePath
      };
    } else if (compress === 'gzip') {
      const tar = await import('tar');
      const archivePath = `${outputDir}.tar.gz`;

      await tar.c(
        {
          gzip: true,
          file: archivePath,
          cwd: dirname(outputDir)
        },
        [path.basename(outputDir)]
      );

      if (job.data.uploadToS3) {
        const { url } = await uploadToS3(archivePath, 'application/gzip', basename(archivePath));
        await rm(dirname(outputDir), { recursive: true, force: true });
        return {
          success: true,
          outputUrl: url
        };
      }

      return {
        success: true,
        outputPath: archivePath
      };
    }

    return {
      success: true,
      outputPaths: frames
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      success: false,
      error: `Failed to extract frames from video: ${errorMessage}`
    };
  }
}
