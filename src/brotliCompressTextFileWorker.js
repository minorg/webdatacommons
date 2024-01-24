/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-var-requires */
/**
 * Worker for brotliCompressTextFile.
 *
 * The code in this file is not actually used. It's copied into a string in brotliCompressTextFile so that the latter
 * doesn't need the path to this file at runtime.
 */
const {parentPort, workerData} = require("node:worker_threads");
const fs = require("node:fs");
const zlib = require("node:zlib");
const {pipeline} = require("node:stream");

const inputFilePath = workerData;
const outputFilePath = inputFilePath + ".br";

const brotliCompressParams = {};
brotliCompressParams[zlib.constants.BROTLI_PARAM_MODE] =
  zlib.constants.BROTLI_MODE_TEXT;
const brotliCompressStream = zlib.createBrotliCompress(brotliCompressParams);

const inputFileStream = fs.createReadStream(inputFilePath);

const outputFileStream = fs.createWriteStream(outputFilePath, {flags: "w+"});

pipeline(inputFileStream, brotliCompressStream, outputFileStream, (error) => {
  if (error) {
    parentPort.postMessage({error, type: "error"});
  } else {
    parentPort.postMessage({outputFilePath, type: "success"});
  }
});
