import HttpClient from "../src/HttpClient.js";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import concat from "concat-stream";
import {Stream} from "node:stream";

const streamToBuffer = (stream: Stream): Promise<Buffer> => {
  return new Promise((resolve, reject) => {
    const concatStream = concat(resolve);
    stream.on("error", reject);
    stream.pipe(concatStream);
  });
};

describe("HttpClient", () => {
  let sut: HttpClient;
  const url = "http://minorgordon.net/index.html";
  let cacheDirPath: string;
  let cacheFilePath: string;

  afterEach(() => {
    fs.rmSync(cacheDirPath, {recursive: true});
  });

  beforeEach(() => {
    cacheDirPath = fs.mkdtempSync(path.join(os.tmpdir(), "HttpClient.test"));
    cacheFilePath = path.join(
      cacheDirPath,
      "http",
      "minorgordon.net",
      "index.html"
    );
    sut = new HttpClient({cacheDirectoryPath: cacheDirPath});
  });

  it(
    "gets a text file twice, hitting the cache the second time",
    async () => {
      const networkHtml = (await streamToBuffer(await sut.get(url))).toString(
        "utf8"
      );
      expect(networkHtml.startsWith("<!DOCTYPE html>")).toBe(true);
      expect(fs.existsSync(path.join(cacheFilePath + ".br"))).toBe(true);

      const cacheHtml = (await streamToBuffer(await sut.get(url))).toString(
        "utf8"
      );
      expect(cacheHtml).toStrictEqual(networkHtml);
    },
    30 * 1000
  );

  it(
    "gets a binary file twice, hitting the cache the second time",
    async () => {
      const url = "https://minorgordon.net/favicon-16x16.png";
      const networkData = await streamToBuffer(await sut.get(url));
      const cacheData = await streamToBuffer(await sut.get(url));
      expect(networkData.equals(cacheData)).toBe(true);
    },
    30 * 1000
  );
});
