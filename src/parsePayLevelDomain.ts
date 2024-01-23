import {ParseResultType, parseDomain} from "parse-domain";

export default function parsePayLevelDomain(domain: string): string | null {
  const parsedDomain = parseDomain(domain);
  if (parsedDomain.type !== ParseResultType.Listed) {
    return null;
  }
  return parsedDomain.domain + "." + parsedDomain.topLevelDomains.join(".");
}
