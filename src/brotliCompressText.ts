import zlib from "node:zlib";

const brotliCompressText = (buffer: Buffer): Promise<Buffer> => {
  const brotliCompressParams: Record<number, number> = {};
  brotliCompressParams[zlib.constants.BROTLI_PARAM_MODE] =
    zlib.constants.BROTLI_MODE_TEXT;
  return new Promise((resolve, reject) => {
    zlib.brotliCompress(
      buffer,
      {params: brotliCompressParams},
      (error, result) => {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      }
    );
  });
};

export default brotliCompressText;
