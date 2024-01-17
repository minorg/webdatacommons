import got, {ExtendOptions, Got, OptionsOfTextResponseBody} from "got";
import path from "node:path";
import fs from "node:fs/promises";
import invariant from "ts-invariant";
import contentTypeParser from "content-type";
import brotliDecompress from "./brotliDecompress.js";
import brotliCompressText from "./brotliCompressText.js";

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

  async get(
    url: string,
    gotOptions?: OptionsOfTextResponseBody
  ): Promise<Buffer> {
    let cacheFilePath = this.cacheFilePath(url);
    try {
      // Common case, compressed text
      return await brotliDecompress(await fs.readFile(cacheFilePath + ".br"));
    } catch {
      try {
        // Uncommon case, uncompressed data
        return await fs.readFile(cacheFilePath);
      } catch {
        /* empty */
      }
    }

    switch (process.env.NODE_ENV) {
      case "development":
      case "test":
        break;
      default:
        throw new Error(
          `refusing to make network request for ${url} in environment ${process.env.NODE_ENV} and ${cacheFilePath} does not exist`
        );
    }

    const response = await this.got.get(url, gotOptions);

    let cacheFileContents = response.rawBody;

    const contentTypeHeader = response.headers["content-type"];
    if (!contentTypeHeader) {
      throw new RangeError("response has no Content-Type header");
    }
    if (contentTypeHeader && isTextResponseBody({contentTypeHeader, url})) {
      cacheFileContents = await brotliCompressText(cacheFileContents);
      cacheFilePath += ".br";
    }

    await fs.mkdir(path.dirname(cacheFilePath), {recursive: true});
    await fs.writeFile(cacheFilePath, cacheFileContents);

    return response.rawBody;
  }
}
