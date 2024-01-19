/* eslint-disable @typescript-eslint/no-namespace */
import {
  NodeType,
  HTMLElement,
  TextNode,
  parse as parseHtml,
} from "node-html-parser";
import {Memoize} from "typescript-memoize";
import HttpClient from "./HttpClient.js";
import {Parser, Store} from "n3";
import Papa from "papaparse";
import {DatasetCore, NamedNode} from "@rdfjs/types";

// Utility functions
const getChildTextNodes = (htmlElement: HTMLElement) =>
  htmlElement.childNodes.filter(
    (childNode) => childNode.nodeType === NodeType.TEXT_NODE
  ) as TextNode[];

const parseGeneralStatsTextNode = (textNode: TextNode) =>
  parseInt(textNode.text.trim().split(" ", 2)[1].replaceAll(",", ""));

const parsePldStatsPropertiesAndDensity = (
  json: string | undefined
): Record<string, number> => {
  if (!json) {
    return {};
  }

  try {
    return JSON.parse(json.replaceAll("'", '"'));
  } catch {
    return {};
  }
};

const parseRelatedClassTextNode = (
  textNode: TextNode
): SchemaDotOrgDataSet.ClassSubset.RelatedClass => {
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

class SchemaDotOrgDataSet {
  private readonly httpClient: HttpClient;
  readonly version: string;

  constructor({
    httpClient,
    version,
  }: {
    httpClient: HttpClient;
    version: string;
  }) {
    this.httpClient = httpClient;
    this.version = version ?? "2022-12";
  }

  @Memoize()
  async classSubsets(): Promise<readonly SchemaDotOrgDataSet.ClassSubset[]> {
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

        const lookupHref =
          tableCells[4].getElementsByTagName("a")[0].attributes["href"];

        const pldStatsHref =
          tableCells[4].getElementsByTagName("a")[1].attributes["href"];

        return new SchemaDotOrgDataSet.ClassSubset({
          className: tableRow.getElementsByTagName("th")[0].text,
          downloadHref: downloadHrefs[0],
          generalStats: {
            hosts: generalStatsHosts,
            quads: generalStatsQuads,
            urls: generalStatsUrls,
          },
          httpClient: this.httpClient,
          lookupHref,
          pldStatsHref,
          relatedClasses,
          sampleDownloadHref: downloadHrefs[1],
          size: sizeCell.text,
        });
      });
  }

  @Memoize()
  async classSubsetsByClassName(): Promise<
    Record<string, SchemaDotOrgDataSet.ClassSubset>
  > {
    return (await this.classSubsets()).reduce(
      (map, classSubset) => {
        map[classSubset.className] = classSubset;
        return map;
      },
      {} as Record<string, SchemaDotOrgDataSet.ClassSubset>
    );
  }
}

namespace SchemaDotOrgDataSet {
  export class ClassSubset {
    readonly className: string;
    readonly generalStats: SchemaDotOrgDataSet.ClassSubset.GeneralStats;
    private readonly httpClient: HttpClient;
    private readonly pldStatsHref: string;
    private readonly lookupHref: string;
    readonly relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
    private readonly sampleDownloadHref: string;
    readonly size: string;

    constructor({
      className,
      generalStats,
      httpClient,
      relatedClasses,
      lookupHref,
      pldStatsHref,
      sampleDownloadHref,
      size,
    }: {
      className: string;
      downloadHref: string;
      generalStats: {
        hosts: number;
        quads: number;
        urls: number;
      };
      httpClient: HttpClient;
      lookupHref: string;
      pldStatsHref: string;
      relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
      sampleDownloadHref: string;
      size: string;
    }) {
      this.className = className;
      this.generalStats = generalStats;
      this.httpClient = httpClient;
      this.lookupHref = lookupHref;
      this.pldStatsHref = pldStatsHref;
      this.relatedClasses = relatedClasses;
      this.sampleDownloadHref = sampleDownloadHref;
      this.size = size;
    }

    private async lookupCsvString(): Promise<string> {
      return (
        await this.httpClient.get(this.lookupHref, {
          //   cache: {
          //     ttl: 31556952000, // 1 year
          //   },
          // })
        })
      ).toString("utf8");
    }

    @Memoize()
    async pldFileNames(): Promise<Record<string, string>> {
      return Papa.parse(await this.lookupCsvString(), {
        header: true,
      }).data.reduce((map: Record<string, string>, row: any) => {
        if (row["pld"].length > 0) {
          map[row["pld"]] = row["file_lookup"];
        }
        return map;
      }, {});
    }

    @Memoize()
    async payLevelDomainSubsets(): Promise<
      readonly SchemaDotOrgDataSet.ClassSubset.PayLevelDomainSubset[]
    > {
      return Papa.parse(await this.pldStatsCsvString(), {
        delimiter: "\t",
        header: true,
      }).data.flatMap((row: any) =>
        row["Domain"].length > 0
          ? [
              {
                domain: row["Domain"] as string,
                stats: {
                  entitiesOfClass: parseInt(row["#Entities of class"]),
                  propertiesAndDensity: parsePldStatsPropertiesAndDensity(
                    row["Properties and Density"]
                  ),
                  quadsOfSubset: parseInt(row["#Quads of Subset"]),
                },
              },
            ]
          : []
      );
    }

    private async pldStatsCsvString(): Promise<string> {
      switch (this.className) {
        case "CreativeWork":
        case "LocalBusiness":
        case "Organization":
        case "Person":
        case "Product":
          // Skip large PLD stats files
          return "";
      }

      return (
        await this.httpClient.get(this.pldStatsHref, {
          //   cache: {
          //     ttl: 31556952000, // 1 year
          //   },
          // })
        })
      ).toString("utf8");
    }

    private async sampleNquadsString(): Promise<string> {
      return (
        await this.httpClient.get(this.sampleDownloadHref, {
          // cache: {
          //   ttl: 31556952000, // 1 year
          // },
        })
      ).toString("utf8");
    }

    @Memoize()
    async samplePagesByIri(): Promise<
      Record<string, SchemaDotOrgDataSet.ClassSubset.PageSubset>
    > {
      const result: Record<string, SchemaDotOrgDataSet.ClassSubset.PageSubset> =
        {};
      const parser = new Parser({format: "N-Quads"});
      parser.parse(await this.sampleNquadsString(), (error, quad) => {
        if (error) {
          return;
          // throw error;
        } else if (!quad) {
          return;
        }
        const pageIri = quad.graph;
        if (pageIri.termType !== "NamedNode") {
          return;
        }
        let page = result[pageIri.value];
        if (!page) {
          page = result[pageIri.value] =
            new SchemaDotOrgDataSet.ClassSubset.PageSubset({
              dataset: new Store(),
              pageIri,
            });
        }
        (page.dataset as Store).addQuad(quad);
      });
      return result;
    }
  }

  export namespace ClassSubset {
    export interface GeneralStats {
      hosts: number;
      quads: number;
      urls: number;
    }

    export class PageSubset {
      readonly dataset: DatasetCore;
      readonly pageIri: NamedNode;

      constructor({
        dataset,
        pageIri,
      }: {
        dataset: DatasetCore;
        pageIri: NamedNode;
      }) {
        this.dataset = dataset;
        this.pageIri = pageIri;
      }
    }

    export class PayLevelDomainSubset {
      readonly domain: string;
      readonly stats: PayLevelDomainSubset.Stats;

      constructor({
        domain,
        stats,
      }: {
        domain: string;
        stats: PayLevelDomainSubset.Stats;
      }) {
        this.domain = domain;
        this.stats = stats;
      }
    }

    export namespace PayLevelDomainSubset {
      export interface Stats {
        readonly entitiesOfClass: number;
        readonly propertiesAndDensity: Record<string, number>;
        readonly quadsOfSubset: number;
      }
    }

    export interface RelatedClass {
      count: number;
      name: string;
      nameLowerCase: string;
    }
  }
}

export default SchemaDotOrgDataSet;
