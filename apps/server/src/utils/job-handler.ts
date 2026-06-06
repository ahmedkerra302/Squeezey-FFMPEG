import { z } from 'zod';
import { randomUUID } from 'crypto';
import { createWriteStream, createReadStream } from 'fs';
import { mkdir, readFile, rm, stat } from 'fs/promises';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import path from 'path';
import { env } from '~/config/env';
import { logger } from '~/config/logger';
import { addJob, queueEvents, validateJobResult, JobTypeName } from '~/queue';
import { computeCacheKey, getCachedOutput, putCachedOutput, isCacheEligibleJobData } from '~/utils/cache';

// Only buffer/cache outputs up to this size. Larger outputs are streamed
// straight from disk to avoid OOM-killing the server process under
// Railway's default memory limit.
const MAX_INMEMORY_BYTES = 16 * 1024 * 1024; // 16 MB

const JobPathsSchema = z.object({
  inputPath: z.string(),
  outputPath: z.string(),
  jobDir: z.string()
});

type JobPaths = z.infer<typeof JobPathsSchema>;

const ProcessJobOptionsSchema = z.object({
  file: z.file(),
  jobType: z.string() as z.ZodType<JobTypeName>,
  outputExtension: z.string().min(1),
  jobData: z.function({
    input: [JobPathsSchema],
    output: z.record(z.string(), z.unknown())
  })
});

type ProcessJobOptions = z.infer<typeof ProcessJobOptionsSchema>;

type ProcessJobResult =
  | {
      success: true;
      outputPath?: string;
      outputUrl?: string;
      outputBuffer?: Buffer;
      outputStream?: NodeJS.ReadableStream;
      outputSize?: number;
      cleanup?: () => Promise<void>;
      metadata?: Record<string, unknown>;
    }
  | {
      success: false;
      error: string;
    };

export async function processMediaJob(options: ProcessJobOptions): Promise<ProcessJobResult> {
  const validated = ProcessJobOptionsSchema.safeParse(options);
  if (!validated.success) {
    return {
      success: false,
      error: `Invalid options: ${validated.error.message}`
    };
  }

  const { file, jobType, outputExtension, jobData } = validated.data;

  const jobId = randomUUID();
  const jobDir = path.join(env.TEMP_DIR, jobId);
  const inputPath = path.join(jobDir, 'input');
  const outputPath = path.join(jobDir, `output.${outputExtension}`);

  const cleanup = async () => {
    await rm(jobDir, { recursive: true, force: true });
  };

  let success = false;

  try {
    const paths: JobPaths = { inputPath, outputPath, jobDir };
    const payload = jobData(paths);

    await mkdir(jobDir, { recursive: true });

    // Stream the upload straight to disk. The previous implementation
    // called `await file.arrayBuffer()` which loaded the entire upload
    // (potentially 100+ MB) into RSS and SIGKILL'd the server process
    // on Railway's default 512 MB limit.
    const inputStream = file.stream() as unknown as ReadableStream<Uint8Array>;
    await pipeline(
      Readable.fromWeb(inputStream as never),
      createWriteStream(inputPath)
    );

    const inputStat = await stat(inputPath);
    const inputSize = inputStat.size;

    // Caching requires the whole input in memory to hash. Only attempt
    // it for small inputs; for everything else we just skip the cache.
    const canUseCache =
      env.CACHE_ENABLED &&
      isCacheEligibleJobData(payload) &&
      inputSize <= MAX_INMEMORY_BYTES;
    let cacheKey: string | null = null;
    if (canUseCache) {
      const inputBuffer = await readFile(inputPath);
      cacheKey = computeCacheKey(inputBuffer, jobType, outputExtension, payload);
      const cached = await getCachedOutput(cacheKey);
      if (cached) {
        logger.info(
          { jobType, outputExtension, cacheKey },
          'Stateless binary cache hit'
        );
        success = true;
        return {
          success: true,
          outputBuffer: cached.outputBuffer,
          outputSize: cached.outputBuffer.length,
          metadata: cached.metadata,
          cleanup
        };
      }
    }

    const job = await addJob(jobType, payload);
    const rawResult = await job.waitUntilFinished(queueEvents);
    const result = validateJobResult(rawResult);

    if (!result.success) {
      return { success: false, error: result.error ?? 'Unknown error' };
    }

    if (result.outputUrl) {
      success = true;
      return {
        success: true,
        outputUrl: result.outputUrl,
        metadata: result.metadata,
        cleanup
      };
    }

    if (result.outputPath) {
      const outputStat = await stat(result.outputPath);
      const outputSize = outputStat.size;

      // Small output -> buffer (so we can cache); large -> stream.
      if (cacheKey && outputSize <= MAX_INMEMORY_BYTES) {
        const outputBuffer = await readFile(result.outputPath);
        await putCachedOutput(cacheKey, outputBuffer, result.metadata);
        success = true;
        return {
          success: true,
          outputPath: result.outputPath,
          outputBuffer,
          outputSize,
          metadata: result.metadata,
          cleanup
        };
      }

      success = true;
      return {
        success: true,
        outputPath: result.outputPath,
        outputStream: createReadStream(result.outputPath),
        outputSize,
        metadata: result.metadata,
        cleanup
      };
    }

    return { success: false, error: 'No output produced' };
  } catch (error) {
    let errorMessage: string;
    if (error instanceof Error) {
      errorMessage = error.message;
    } else {
      errorMessage = String(error);
    }
    return { success: false, error: errorMessage };
  } finally {
    // Only auto-clean on failure. On success, the caller is responsible
    // for invoking result.cleanup() AFTER it has finished streaming the
    // output file back to the client.
    if (!success) {
      await cleanup();
    }
  }
}

export function getOutputFilename(originalName: string, newExtension: string): string {
  const baseName = originalName.replace(/\.[^.]+$/, '');
  if (newExtension) {
    return `${baseName}.${newExtension}`;
  }
  return baseName;
}
