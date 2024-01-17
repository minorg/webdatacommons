import zlib from "node:zlib";

export default function brotliDecompress(buffer: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    zlib.brotliDecompress(buffer, (error, result) => {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
}
