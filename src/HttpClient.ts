import got, {ExtendOptions, Got, Response} from "got";
import path from "node:path";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import {invariant} from "ts-invariant";
import contentTypeParser from "content-type";
import {Stream} from "node:stream";
import zlib from "node:zlib";
import {pipeline as streamPipeline} from "node:stream/promises";
import {pino} from "pino";
import process from "node:process";

const logger = pino({
  level:
    process.env.NODE_ENV === "development" || process.env.NODE_ENV === "test"
      ? "debug"
      : "info",
});

const isTextResponseBody = ({
  contentTypeHeader,
  url,
}: {
  contentTypeHeader: string;
  url: string;
}): boolean => {
  const contentType = contentTypeParser
    .parse(contentTypeHeader)
    .type.toLowerCase();
  if (contentType.startsWith("text/")) {
    return true;
  } else if (contentType.startsWith("image/")) {
    return false;
  }

  switch (contentType) {
    case "application/json":
      return true;
    case "application/octet-stream":
      return url.endsWith(".csv");
    default:
      throw new RangeError("unrecognized Content-Type: " + contentType);
  }
};

/**
 * Facade for a simple HTTP client used for fetching data from WebDataCommons and other sites.
 *
 * The client:
 * - only supports GET
 * - caches response bodies indefinitely on the file system, ignoring RFC 9213 cache control
 * - throws an exception if a network request would be made in NODE_ENV=production (i.e., from GitHub Pages, where new files wouldn't be added to the committed cache)
 */
export default class HttpClient {
  private readonly cacheDirectoryPath;
  private readonly got: Got;

  constructor({
    cacheDirectoryPath,
    gotOptions,
  }: {
    cacheDirectoryPath: string;
    gotOptions?: ExtendOptions;
  }) {
    this.cacheDirectoryPath = cacheDirectoryPath;
    this.got = gotOptions ? got.extend(gotOptions) : got;
  }

  private cacheFilePath(url: string): string {
    const parsedUrl = new URL(url);
    invariant(parsedUrl.hash.length === 0);
    invariant(parsedUrl.password.length === 0);
    invariant(parsedUrl.port.length === 0);
    invariant(parsedUrl.search.length === 0);
    invariant(parsedUrl.username.length === 0);

    return path.join(
      this.cacheDirectoryPath,
      parsedUrl.protocol.substring(0, parsedUrl.protocol.length - 1),
      parsedUrl.host,
      parsedUrl.pathname
    );
  }

  async get(url: string): Promise<Stream> {
    const cachedStream = this.getCached(url);
    if (cachedStream !== null) {
      logger.info("HTTP client cache hit: %s", url);
      return cachedStream;
    }
    logger.info("HTTP client cache miss: %s", url);

    switch (process.env.NODE_ENV) {
      case "development":
      case "test":
        break;
      default:
        throw new Error(
          `refusing to make network request for ${url} in environment ${process.env.NODE_ENV}`
        );
    }

    await this.getUncached(url);

    const cacheFileStream = this.getCached(url);
    invariant(cacheFileStream, "must exist here");
    return cacheFileStream!;
  }

  private getCached(url: string): Stream | null {
    // Checking exists then creating a read stream is not ideal,
    // but fs.createReadStream doesn't report an error until the stream is read by the caller.
    // It should be OK, since the cache is supposed to be immutable.

    let cacheFilePath = this.cacheFilePath(url);
    if (fs.existsSync(cacheFilePath)) {
      logger.debug("cache file exists: %s", cacheFilePath);
      return fs.createReadStream(cacheFilePath);
    }

    logger.debug("no such cache file: %s", cacheFilePath);
    cacheFilePath += ".br";
    if (fs.existsSync(cacheFilePath)) {
      logger.debug("cache file exists: %s", cacheFilePath);
      return fs
        .createReadStream(cacheFilePath)
        .pipe(zlib.createBrotliDecompress());
    }

    return null;
  }

  private async getUncached(url: string): Promise<void> {
    logger.debug("requesting %s", url);
    const requestStream = this.got.stream(url);
    return new Promise((resolve, reject) => {
      requestStream.on("downloadProgress", ({transferred, total, percent}) => {
        const percentage = Math.round(percent * 100);
        logger.info(
          `${url} download: ${transferred}/${total} (${percentage}%)`
        );
      });

      requestStream.on("response", async (response: Response) => {
        const contentTypeHeader = response.headers["content-type"];
        if (!contentTypeHeader) {
          throw new RangeError("response has no Content-Type header");
        }
        logger.debug("%s Content-Type: %s", url, contentTypeHeader);
        const compress =
          contentTypeHeader && isTextResponseBody({contentTypeHeader, url});

        let cacheFilePath = this.cacheFilePath(url);
        if (compress) {
          cacheFilePath += ".br";
        }
        logger.debug("%s cache file path: %s", url, cacheFilePath);

        requestStream.off("error", reject);

        const cacheDirPath = path.dirname(cacheFilePath);
        logger.debug("recursively creating directory: %s", cacheDirPath);
        await fsPromises.mkdir(cacheDirPath, {recursive: true});
        logger.debug("recursively created directory: %s", cacheDirPath);

        const cacheFileStream = fs.createWriteStream(cacheFilePath);

        logger.debug("waiting on stream pipeline to %s", cacheFilePath);
        if (compress) {
          const brotliCompressParams: Record<number, number> = {};
          brotliCompressParams[zlib.constants.BROTLI_PARAM_MODE] =
            zlib.constants.BROTLI_MODE_TEXT;
          await streamPipeline(
            requestStream,
            zlib.createBrotliCompress(brotliCompressParams),
            cacheFileStream
          );
        } else {
          await streamPipeline(requestStream, cacheFileStream);
        }
        logger.debug("finished stream pipeline to %s", cacheFilePath);

        resolve();
      });

      requestStream.once("error", reject);
    });
  }
}
