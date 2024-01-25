import {pino} from "pino";

const logger = pino({
  level:
    process.env.NODE_ENV === "development" || process.env.NODE_ENV === "test"
      ? "debug"
      : "info",
});

export default logger;
