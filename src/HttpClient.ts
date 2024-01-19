import got, {ExtendOptions, Got, Response} from "got";
import path from "node:path";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import {invariant} from "ts-invariant";
import contentTypeParser from "content-type";
import {Stream} from "node:stream";
import zlib from "node:zlib";
import {pipeline as streamPipeline} from "node:stream/promises";

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
    try {
      return this.getCached(url);
    } catch {
      /* empty */
    }

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

    return this.getCached(url);
  }

  private getCached(url: string): Stream {
    let cacheFilePath = this.cacheFilePath(url);
    try {
      return fs.createReadStream(cacheFilePath);
    } catch {
      cacheFilePath += ".br";
      return fs
        .createReadStream(cacheFilePath)
        .pipe(zlib.createBrotliDecompress());
    }
  }

  private async getUncached(url: string): Promise<void> {
    const requestStream = this.got.stream(url);
    requestStream.on("response", async (response: Response) => {
      const contentTypeHeader = response.headers["content-type"];
      if (!contentTypeHeader) {
        throw new RangeError("response has no Content-Type header");
      }
      const compress =
        contentTypeHeader && isTextResponseBody({contentTypeHeader, url});

      let cacheFilePath = this.cacheFilePath(url);
      if (compress) {
        cacheFilePath += ".br";
      }

      requestStream.off("error", (error) => {
        throw error;
      });

      await fsPromises.mkdir(path.dirname(cacheFilePath), {recursive: true});

      const cacheFileStream = fs.createWriteStream(cacheFilePath);

      if (compress) {
        await streamPipeline(
          requestStream,
          zlib.createBrotliCompress(),
          cacheFileStream
        );
      } else {
        await streamPipeline(requestStream, cacheFileStream);
      }
    });

    requestStream.once("error", (error) => {
      throw error;
    });
  }
}
