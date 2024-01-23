import ImmutableCache from "../src/ImmutableCache.js";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import {Readable} from "node:stream";
import streamToBuffer from "../src/streamToBuffer.js";

const stringToStream = (s: string): Readable => {
  return Readable.from(Buffer.from(s, "utf8"));
};

const streamToString = async (s: Readable): Promise<string> => {
  return (await streamToBuffer(s)).toString("utf8");
};

describe("ImmutableCache", () => {
  let sut: ImmutableCache;
  let cacheDirPath: string;
  const key: ImmutableCache.Key = ["a", "b"];

  afterEach(() => {
    fs.rmSync(cacheDirPath, {recursive: true});
  });

  beforeEach(() => {
    cacheDirPath = fs.mkdtempSync(path.join(os.tmpdir(), "HttpClient.test"));
    sut = new ImmutableCache({rootDirectoryPath: cacheDirPath});
  });

  it("returns null from a missing key", async () => {
    expect(await sut.get(key)).toBeNull();
  });

  it("sets a value", async () => {
    await sut.set(key, await stringToStream("test"));
  });

  it("sets a value and gets it back", async () => {
    await sut.set(key, await stringToStream("test"));
    const value = await sut.get(key);
    expect(value).not.toBeNull();
    expect(await streamToString(value!)).toStrictEqual("test");
  });

  it("sets a value twice and gets second value back", async () => {
    await sut.set(key, await stringToStream("test"));
    await sut.set(key, await stringToStream("test2"));
    const value = await sut.get(key);
    expect(value).not.toBeNull();
    expect(await streamToString(value!)).toStrictEqual("test2");
  });
});
