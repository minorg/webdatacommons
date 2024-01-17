import {
  NodeType,
  HTMLElement,
  TextNode,
  parse as parseHtml,
} from "node-html-parser";
import SchemaDotOrgDataSetClassSpecificSubset from "./SchemaDotOrgDataSetClassSpecificSubset";
import {Memoize} from "typescript-memoize";
import path from "node:path";
import {dataDirPath} from "@/lib/paths";
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import HttpClient from "./HttpClient";
import SchemaDotOrgRelatedClass from "./SchemaDotOrgRelatedClass";

// Utility functions
const getChildTextNodes = (htmlElement: HTMLElement) =>
  htmlElement.childNodes.filter(
    (childNode) => childNode.nodeType === NodeType.TEXT_NODE
  ) as TextNode[];

const parseGeneralStatsTextNode = (textNode: TextNode) =>
  parseInt(textNode.text.trim().split(" ", 2)[1].replaceAll(",", ""));

const parseRelatedClassTextNode = (
  textNode: TextNode
): SchemaDotOrgRelatedClass => {
  const [iri, count] = textNode.text.trim().split(" ", 2);

  let name: string;
  if (iri.startsWith("http://schema.org/")) {
    name = iri.substring("http://schema.org/".length);
  } else if (iri.startsWith("https://schema.org/")) {
    name = iri.substring("https://schema.org/".length);
  } else {
    throw new RangeError(iri);
  }

  return {
    count: parseInt(count.substring(1, count.length - 1).replaceAll(",", "")),
    name,
    nameLowerCase: name.toLowerCase(),
  };
};

export default class SchemaDotOrgCorpus {
  private readonly httpClient: HttpClient;
  readonly version: string;

  constructor({version}: {version?: string}) {
    this.httpClient = new HttpClient({
      cacheDirectoryPath: path.resolve(
        dataDirPath,
        "webdatacommons",
        "http-cache"
      ),
      gotOptions: {
        retry: {
          limit: 10,
        },
      },
    });

    this.version = version ?? "2022-12";
  }

  @Memoize()
  async classSpecificSubsets(): Promise<
    readonly SchemaDotOrgDataSetClassSpecificSubset[]
  > {
    const metadataHtml: string = (
      await this.httpClient.get(
        `https://webdatacommons.org/structureddata/${this.version}/stats/schema_org_subsets.html`
        // {
        //   cache: {
        //     ttl: 31556952000, // 1 year
        //   },
        // }
      )
    ).toString("utf8");

    return parseHtml(metadataHtml)
      .querySelector("body > div > h2")!
      .parentNode.getElementsByTagName("tr")
      .slice(1)
      .map((tableRow) => {
        const tableCells = tableRow.getElementsByTagName("td");

        const generalStatsTextNodes: TextNode[] = getChildTextNodes(
          tableCells[0]
        );
        const generalStatsQuads = parseGeneralStatsTextNode(
          generalStatsTextNodes[0]
        );
        const generalStatsUrls = parseGeneralStatsTextNode(
          generalStatsTextNodes[1]
        );
        const generalStatsHosts = parseGeneralStatsTextNode(
          generalStatsTextNodes[2]
        );

        const relatedClasses = getChildTextNodes(tableCells[1]).map(
          (textNode) => parseRelatedClassTextNode(textNode)
        );

        const sizeCell = tableCells[2];

        const downloadHrefs = tableCells[3]
          .getElementsByTagName("a")
          .map((anchorElement) => anchorElement.attributes["href"]);

        const pldStatsHref =
          tableCells[4].getElementsByTagName("a")[1].attributes["href"];

        return new SchemaDotOrgDataSetClassSpecificSubset({
          className: tableRow.getElementsByTagName("th")[0].text,
          downloadHref: downloadHrefs[0],
          generalStats: {
            hosts: generalStatsHosts,
            quads: generalStatsQuads,
            urls: generalStatsUrls,
          },
          httpClient: this.httpClient,
          pldStatsHref,
          relatedClasses,
          sampleDownloadHref: downloadHrefs[1],
          size: sizeCell.text,
        });
      });
  }

  @Memoize()
  async classSpecificSubsetsByClassName(): Promise<
    Record<string, SchemaDotOrgDataSetClassSpecificSubset>
  > {
    return (await this.classSpecificSubsets()).reduce(
      (map, classSpecificSubset) => {
        map[classSpecificSubset.className] = classSpecificSubset;
        return map;
      },
      {} as Record<string, SchemaDotOrgDataSetClassSpecificSubset>
    );
  }
}
