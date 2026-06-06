import type { Context } from 'hono';
import type { OpenAPIHono } from '@hono/zod-openapi';
import { Readable } from 'stream';
import {
  videoToMp4Route,
  videoToMp4UrlRoute,
  extractAudioRoute,
  extractAudioUrlRoute,
  extractFramesRoute,
  extractFramesUrlRoute,
  downloadFrameRoute
} from './schemas';
import { videoToGifRoute, videoToGifUrlRoute } from './gif-schemas';
import { JobType } from '~/queue';
import { env } from '~/config/env';
import { processMediaJob, getOutputFilename } from '~/utils/job-handler';

type BinaryResult = {
  outputBuffer?: Buffer;
  outputStream?: NodeJS.ReadableStream;
  outputSize?: number;
  cleanup?: () => Promise<void>;
};

// Send either a buffered or streamed result, and run cleanup() AFTER
// the response body has been fully sent. We rely on the platform fetch
// runtime to drain the ReadableStream before continuing.
function sendBinary(
  c: Context,
  result: BinaryResult,
  contentType: string,
  filename: string,
  fallbackError: string
) {
  const headers: Record<string, string> = {
    'Content-Type': contentType,
    'Content-Disposition': `attachment; filename="${filename}"`
  };
  if (result.outputSize) {
    headers['Content-Length'] = String(result.outputSize);
  }

  if (result.outputBuffer) {
    const response = c.body(new Uint8Array(result.outputBuffer), 200, headers);
    // Buffered path: safe to cleanup immediately, the bytes are in memory.
    result.cleanup?.().catch(() => {});
    return response;
  }

  if (result.outputStream) {
    const nodeStream = result.outputStream;
    // Convert Node Readable -> Web ReadableStream so Hono/the runtime can
    // stream it directly to the client without buffering it again.
    const webStream = Readable.toWeb(nodeStream as Readable) as unknown as ReadableStream<Uint8Array>;

    // Defer cleanup until the stream ends or errors.
    nodeStream.once('end', () => {
      result.cleanup?.().catch(() => {});
    });
    nodeStream.once('error', () => {
      result.cleanup?.().catch(() => {});
    });
    nodeStream.once('close', () => {
      result.cleanup?.().catch(() => {});
    });

    return c.body(webStream, 200, headers);
  }

  result.cleanup?.().catch(() => {});
  return c.json({ error: fallbackError }, 400);
}

export function registerVideoRoutes(app: OpenAPIHono) {
  app.openapi(videoToMp4Route, async (c) => {
    try {
      const { file } = c.req.valid('form');
      // Phase 2: allow callers to override compression strength + max edge.
      // When the query is absent, defaults match the previous deploy
      // (CRF 28, veryfast preset, 1280px long edge).
      const query = c.req.valid('query') as { crf?: number; maxEdge?: number };
      const crf = query.crf ?? 28;
      const maxEdge = query.maxEdge ?? 1280;

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_TO_MP4,
        outputExtension: 'mp4',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          crf,
          preset: 'veryfast',
          smartCopy: false,
          maxEdge
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      return sendBinary(
        c,
        result,
        'video/mp4',
        getOutputFilename(file.name, 'mp4'),
        'Conversion failed'
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(extractAudioRoute, async (c) => {
    try {
      const { file } = c.req.valid('form');
      const query = c.req.valid('query');
      const mono = query.mono === 'yes';

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_EXTRACT_AUDIO,
        outputExtension: 'wav',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          mono
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      return sendBinary(
        c,
        result,
        'audio/wav',
        getOutputFilename(file.name, 'wav'),
        'Audio extraction failed'
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(extractFramesRoute, async (c) => {
    try {
      const { file } = c.req.valid('form');
      const query = c.req.valid('query');
      const fps = query.fps || 1;
      const compress = query.compress;

      if (!compress) {
        return c.json(
          {
            error: 'compress parameter is required',
            message: 'Please specify compress=zip or compress=gzip to get frames as an archive'
          },
          400
        );
      }

      const extension = compress === 'zip' ? 'zip' : 'tar.gz';

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_EXTRACT_FRAMES,
        outputExtension: extension,
        jobData: ({ inputPath, jobDir }) => ({
          inputPath,
          outputDir: `${jobDir}/frames`,
          fps,
          format: 'png',
          compress
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      const contentType = compress === 'zip' ? 'application/zip' : 'application/gzip';
      return sendBinary(
        c,
        result,
        contentType,
        `${getOutputFilename(file.name, '')}_frames.${extension}`,
        'Frame extraction failed'
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(videoToMp4UrlRoute, async (c) => {
    try {
      if (env.STORAGE_MODE !== 's3') {
        return c.json({ error: 'S3 mode not enabled' }, 400);
      }

      const { file } = c.req.valid('form');

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_TO_MP4,
        outputExtension: 'mp4',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          crf: 28,
          preset: 'veryfast',
          smartCopy: false,
          uploadToS3: true
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      result.cleanup?.().catch(() => {});

      if (!result.outputUrl) {
        return c.json({ error: 'Conversion failed' }, 400);
      }

      return c.json({ url: result.outputUrl }, 200);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(extractAudioUrlRoute, async (c) => {
    try {
      if (env.STORAGE_MODE !== 's3') {
        return c.json({ error: 'S3 mode not enabled' }, 400);
      }

      const { file } = c.req.valid('form');
      const query = c.req.valid('query');
      const mono = query.mono === 'yes';

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_EXTRACT_AUDIO,
        outputExtension: 'wav',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          mono,
          uploadToS3: true
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      result.cleanup?.().catch(() => {});

      if (!result.outputUrl) {
        return c.json({ error: 'Audio extraction failed' }, 400);
      }

      return c.json({ url: result.outputUrl }, 200);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(extractFramesUrlRoute, async (c) => {
    try {
      if (env.STORAGE_MODE !== 's3') {
        return c.json({ error: 'S3 mode not enabled' }, 400);
      }

      const { file } = c.req.valid('form');
      const query = c.req.valid('query');
      const fps = query.fps || 1;
      const compress = query.compress;

      if (!compress) {
        return c.json(
          {
            error: 'compress parameter is required',
            message: 'Please specify compress=zip or compress=gzip to get frames as an archive'
          },
          400
        );
      }

      const extension = compress === 'zip' ? 'zip' : 'tar.gz';

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_EXTRACT_FRAMES,
        outputExtension: extension,
        jobData: ({ inputPath, jobDir }) => ({
          inputPath,
          outputDir: `${jobDir}/frames`,
          fps,
          format: 'png',
          compress,
          uploadToS3: true
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      result.cleanup?.().catch(() => {});

      if (!result.outputUrl) {
        return c.json({ error: 'Frame extraction failed' }, 400);
      }

      return c.json({ url: result.outputUrl }, 200);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(downloadFrameRoute, (c) => {
    return c.json(
      {
        error: 'Not implemented - use compress parameter on POST /video/frames instead'
      },
      501
    );
  });

  app.openapi(videoToGifRoute, async (c) => {
    try {
      const { file } = c.req.valid('form');
      const query = c.req.valid('query');

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_TO_GIF,
        outputExtension: 'gif',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          fps: query.fps,
          width: query.width
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      return sendBinary(
        c,
        result,
        'image/gif',
        getOutputFilename(file.name, 'gif'),
        'Conversion failed'
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });

  app.openapi(videoToGifUrlRoute, async (c) => {
    try {
      if (env.STORAGE_MODE !== 's3') {
        return c.json({ error: 'S3 mode not enabled' }, 400);
      }

      const { file } = c.req.valid('form');
      const query = c.req.valid('query');

      const result = await processMediaJob({
        file,
        jobType: JobType.VIDEO_TO_GIF,
        outputExtension: 'gif',
        jobData: ({ inputPath, outputPath }) => ({
          inputPath,
          outputPath,
          fps: query.fps,
          width: query.width,
          uploadToS3: true
        })
      });

      if (!result.success) {
        return c.json({ error: result.error }, 400);
      }

      result.cleanup?.().catch(() => {});

      if (!result.outputUrl) {
        return c.json({ error: 'Conversion failed' }, 400);
      }

      return c.json({ url: result.outputUrl }, 200);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return c.json({ error: 'Processing failed', message: errorMessage }, 500);
    }
  });
}
