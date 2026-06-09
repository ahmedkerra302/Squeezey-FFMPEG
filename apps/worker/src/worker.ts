import { Worker } from 'bullmq';
import { connection, checkRedisHealth } from '@worker/config/redis';
import { env } from '@worker/config/env';
import { logger } from '@worker/config/logger';
import { QUEUE_NAME, JobType } from '@worker/queue';
import type { JobResult } from '@worker/queue';
import { checkS3Health } from '@worker/utils/storage';

import { processAudioToMp3, processAudioToWav } from '@worker/queue/audio/processor';
import { processVideoToMp4, processVideoExtractAudio, processVideoExtractFrames } from '@worker/queue/video/processor';
import { processVideoToGif } from '@worker/queue/video/gif-processor';
import { processImageToJpg, processImageResize } from '@worker/queue/image/processor';
import { processMediaProbe } from '@worker/queue/media/processor';
import { startSupabasePoller } from '@worker/poller/supabase-poller';

await checkRedisHealth();

const worker = new Worker<unknown, JobResult>(
  QUEUE_NAME,
  async (job) => {
    logger.info({ jobId: job.id, jobType: job.name }, 'Processing job');

    switch (job.name) {
      case JobType.AUDIO_TO_MP3:
        return processAudioToMp3(job as never);
      case JobType.AUDIO_TO_WAV:
        return processAudioToWav(job as never);
      case JobType.VIDEO_TO_MP4:
        return processVideoToMp4(job as never);
      case JobType.VIDEO_EXTRACT_AUDIO:
        return processVideoExtractAudio(job as never);
      case JobType.VIDEO_EXTRACT_FRAMES:
        return processVideoExtractFrames(job as never);
      case JobType.VIDEO_TO_GIF:
        return processVideoToGif(job as never);
      case JobType.IMAGE_TO_JPG:
        return processImageToJpg(job as never);
      case JobType.IMAGE_RESIZE:
        return processImageResize(job as never);
      case JobType.MEDIA_PROBE:
        return processMediaProbe(job as never);
      default:
        throw new Error(`Unknown job type: ${job.name}`);
    }
  },
  {
    connection,
    concurrency: env.WORKER_CONCURRENCY,
    // Long video encodes can exceed BullMQ's default 30s lock,
    // marking jobs stalled and re-queueing them forever (pending).
    // PROCESSING_TIMEOUT is 600s, so give the lock 700s of headroom.
    lockDuration: 700_000,
    lockRenewTime: 300_000,
    stalledInterval: 30_000,
    maxStalledCount: 1
  }
);

worker.on('completed', (job, result) => {
  if (result?.success === false) {
    logger.error({ jobId: job.id, error: result.error }, 'Job completed with processing error');
    return;
  }

  logger.info({ jobId: job.id }, 'Job completed successfully');
});

worker.on('failed', (job, err) => {
  logger.error({ jobId: job?.id, error: err.message }, 'Job failed');
});

worker.on('error', (err) => {
  logger.error({ error: err.message }, 'Worker error');
});

logger.info(`🔄 Worker started processing queue: ${QUEUE_NAME}`);
logger.info(`⚙️  Concurrency: ${env.WORKER_CONCURRENCY}`);
logger.info(`💾 Storage Mode: ${env.STORAGE_MODE.toUpperCase()}`);

if (env.STORAGE_MODE === 's3') {
  logger.info(`   S3 Bucket: ${env.S3_BUCKET}`);
  logger.info(`   S3 Region: ${env.S3_REGION}`);
  logger.info(`   S3 Prefix: ${env.S3_PATH_PREFIX}`);
  await checkS3Health();
}

startSupabasePoller();
logger.info('🗄️  Supabase database poller started');
