import path from "node:path";
import {Worker} from "node:worker_threads";

type WorkerMessage =
  | {error: any; type: "error"}
  | {outputFilePath: string; type: "success"};

export default function (filePath: string) {
  return new Promise<string>((resolve, reject) => {
    const worker = new Worker(
      path.resolve(".", "dist", "brotliCompressTextFileWorker.js"),
      {
        workerData: filePath,
      }
    );
    worker.on("message", (message: WorkerMessage) => {
      if (message.type === "error") {
        reject(message.error);
      } else {
        resolve(message.outputFilePath);
      }
    });
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (code !== 0)
        reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}
