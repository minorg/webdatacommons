import {Worker} from "node:worker_threads";

// Worker code copied from brotliCompressTextFileWorker.js, so we don't have to know the path to that file at runtime.
const workerJs = `
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
`;

type WorkerMessage =
  | {error: any; type: "error"}
  | {outputFilePath: string; type: "success"};

export default function (filePath: string) {
  return new Promise<string>((resolve, reject) => {
    const worker = new Worker(workerJs, {
      eval: true,
      workerData: filePath,
    });
    worker.on("message", (message: WorkerMessage) => {
      if (message.type === "error") {
        reject(message.error);
      } else {
        resolve(message.outputFilePath);
      }
    });
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}
