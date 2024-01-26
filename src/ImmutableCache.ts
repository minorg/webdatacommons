import fs from "node:fs";
import fsPromises from "node:fs/promises";
import {Readable} from "node:stream";
import {pipeline as streamPipeline} from "node:stream/promises";
import path from "node:path";
import logger from "./logger.js";
import invariant from "ts-invariant";

/**
 * Immutable cache backed by the file system.
 */
class ImmutableCache {
  private readonly readonly: boolean;
  private readonly rootDirectoryPath: string;

  constructor({
    readonly,
    rootDirectoryPath,
  }: {
    readonly?: boolean;
    rootDirectoryPath: string;
  }) {
    this.readonly = !!readonly;
    this.rootDirectoryPath = rootDirectoryPath;
  }

  async createWriteStream(key: ImmutableCache.Key): Promise<fs.WriteStream> {
    const filePath = this.filePath(key);

    if (this.readonly) {
      throw new Error(
        `Cache is read-only: refusing to create write stream for ${filePath} in environment ${process.env.NODE_ENV}`
      );
    }

    await this.mkdirs(key);
    return fs.createWriteStream(filePath, {flags: "w+"});
  }

  private filePath(key: ImmutableCache.Key): string {
    return path.join(this.rootDirectoryPath, ...key);
  }

  async get(key: ImmutableCache.Key): Promise<ImmutableCache.Value | null> {
    const filePath = this.filePath(key);

    // Checking exists then creating a read stream is not ideal,
    // but fs.createReadStream doesn't report an error until the stream is read by the caller.
    // It should be OK, since the cache is supposed to be immutable.
    const fileExists = !!(await fsPromises.stat(filePath).catch(() => false));
    if (!fileExists) {
      logger.debug("cache miss: %s", key.join("/"));
      return null;
    }

    logger.debug("cache hit: %s", key.join("/"));

    return fs.createReadStream(filePath);
  }

  private async mkdirs(key: ImmutableCache.Key): Promise<void> {
    invariant(!this.readonly);
    const dirPath = path.dirname(this.filePath(key));
    // logger.debug("recursively creating directory: %s", dirPath);
    await fsPromises.mkdir(dirPath, {recursive: true});
    // logger.debug("recursively created directory: %s", dirPath);
  }

  async set(
    key: ImmutableCache.Key,
    value: ImmutableCache.Value
  ): Promise<void> {
    const filePath = this.filePath(key);

    if (this.readonly) {
      throw new Error(
        `Cache is read-only: refusing to write to ${filePath} in environment ${process.env.NODE_ENV}`
      );
    }

    await this.mkdirs(key);
    const fileStream = fs.createWriteStream(filepath);
    try {
      await streamPipeline([value, fileStream]);
    } finally {
      fileStream.close();
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace
namespace ImmutableCache {
  export type Key = readonly string[];
  export type Value = Readable;
}

export default ImmutableCache;
