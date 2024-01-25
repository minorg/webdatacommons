import got, {ExtendOptions, Got, Response} from "got";
import {invariant} from "ts-invariant";
import contentTypeParser from "content-type";
import {Readable} from "node:stream";
import zlib from "node:zlib";
import process from "node:process";
import logger from "./logger.js";
import cliProgress from "cli-progress";
import ImmutableCache from "./ImmutableCache.js";

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
  private readonly cache: ImmutableCache;
  private readonly got: Got;

  constructor({
    cache,
    gotOptions,
  }: {
    cache: ImmutableCache;
    gotOptions?: ExtendOptions;
  }) {
    this.cache = cache;
    this.got = gotOptions ? got.extend(gotOptions) : got;
  }

  private cacheKey(url: string, pathSuffix?: string): ImmutableCache.Key {
    const parsedUrl = new URL(url);
    invariant(parsedUrl.hash.length === 0);
    invariant(parsedUrl.password.length === 0);
    invariant(parsedUrl.port.length === 0);
    invariant(parsedUrl.search.length === 0);
    invariant(parsedUrl.username.length === 0);

    return [
      "http-client",
      parsedUrl.protocol.substring(0, parsedUrl.protocol.length - 1),
      parsedUrl.host,
      parsedUrl.pathname + (pathSuffix ?? ""),
    ];
  }

  async get(url: string): Promise<Readable> {
    const cachedStream = await this.getCached(url);
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

    const cacheFileStream = await this.getCached(url);
    invariant(cacheFileStream, "must exist here");
    return cacheFileStream!;
  }

  private async getCached(url: string): Promise<Readable | null> {
    {
      const cacheValue = await this.cache.get(this.cacheKey(url));
      if (cacheValue !== null) {
        return cacheValue;
      }
    }

    {
      const cacheValue = await this.cache.get(this.cacheKey(url, ".br"));
      if (cacheValue !== null) {
        return cacheValue.pipe(zlib.createBrotliDecompress());
      }
    }

    return null;
  }

  private async getUncached(url: string): Promise<void> {
    logger.debug("requesting %s", url);
    const requestStream = this.got.stream(url);
    const progressBar = new cliProgress.SingleBar({});
    progressBar.start(100, 0);
    return new Promise((resolve, reject) => {
      requestStream.on("downloadProgress", ({percent}) => {
        const percentage = Math.round(percent * 100);
        progressBar.update(percentage);
      });

      requestStream.on("response", async (response: Response) => {
        const contentTypeHeader = response.headers["content-type"];
        if (!contentTypeHeader) {
          throw new RangeError("response has no Content-Type header");
        }
        logger.debug("%s Content-Type: %s", url, contentTypeHeader);
        const compress =
          contentTypeHeader && isTextResponseBody({contentTypeHeader, url});

        requestStream.off("error", reject);

        const cacheKey = this.cacheKey(url, compress ? ".br" : undefined);
        let cacheValue: Readable = requestStream;

        if (compress) {
          const brotliCompressParams: Record<number, number> = {};
          brotliCompressParams[zlib.constants.BROTLI_PARAM_MODE] =
            zlib.constants.BROTLI_MODE_TEXT;
          cacheValue = requestStream.pipe(
            zlib.createBrotliCompress(brotliCompressParams)
          );
        }

        await this.cache.set(cacheKey, cacheValue);

        resolve();
      });

      requestStream.once("error", reject);
    });
  }
}
