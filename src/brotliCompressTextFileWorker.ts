import {parentPort, workerData} from "node:worker_threads";
import fs from "node:fs";
import zlib from "node:zlib";
import {pipeline} from "node:stream";

const inputFilePath = workerData;
const outputFilePath = inputFilePath + ".br";

const brotliCompressParams: Record<number, number> = {};
brotliCompressParams[zlib.constants.BROTLI_PARAM_MODE] =
  zlib.constants.BROTLI_MODE_TEXT;
const brotliCompressStream = zlib.createBrotliCompress(brotliCompressParams);

const inputFileStream = fs.createReadStream(inputFilePath);

const outputFileStream = fs.createWriteStream(outputFilePath, {flags: "w+"});

pipeline(inputFileStream, brotliCompressStream, outputFileStream, (error) => {
  if (error) {
    parentPort!.postMessage({error, type: "error"});
  } else {
    parentPort!.postMessage({outputFilePath, type: "success"});
  }
});
