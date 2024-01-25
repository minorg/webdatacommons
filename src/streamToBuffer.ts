import concat from "concat-stream";
import {Stream} from "node:stream";

export default function streamToBuffer(stream: Stream): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const concatStream = concat(resolve);
    stream.on("error", reject);
    stream.pipe(concatStream);
  });
}
