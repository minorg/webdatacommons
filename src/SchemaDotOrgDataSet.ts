/* eslint-disable @typescript-eslint/no-namespace */
import {
  NodeType,
  HTMLElement,
  TextNode,
  parse as parseHtml,
} from "node-html-parser";
import {Memoize} from "typescript-memoize";
import HttpClient from "./HttpClient.js";
import {Parser, StreamParser, Store} from "n3";
import Papa from "papaparse";
import {DatasetCore, NamedNode} from "@rdfjs/types";
import {invariant} from "ts-invariant";
import zlib from "node:zlib";
import streamToBuffer from "./streamToBuffer.js";

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
      await streamToBuffer(
        await this.httpClient.get(
          `https://webdatacommons.org/structureddata/${this.version}/stats/schema_org_subsets.html`
          // {
          //   cache: {
          //     ttl: 31556952000, // 1 year
          //   },
          // }
        )
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

        const lookupFileUrl =
          tableCells[4].getElementsByTagName("a")[0].attributes["href"];

        const pldStatsFileUrl =
          tableCells[4].getElementsByTagName("a")[1].attributes["href"];

        return new SchemaDotOrgDataSet.ClassSubset({
          className: tableRow.getElementsByTagName("th")[0].text,
          downloadDirectoryUrl: downloadHrefs[0],
          generalStats: {
            hosts: generalStatsHosts,
            quads: generalStatsQuads,
            urls: generalStatsUrls,
          },
          httpClient: this.httpClient,
          lookupFileUrl,
          pldStatsFileUrl,
          relatedClasses,
          sampleDataFileUrl: downloadHrefs[1],
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
    private readonly downloadDirectoryUrl: string;
    readonly generalStats: SchemaDotOrgDataSet.ClassSubset.GeneralStats;
    private readonly httpClient: HttpClient;
    private readonly pldStatsFileUrl: string;
    private readonly lookupFileUrl: string;
    readonly relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
    private readonly sampleDataFileUrl: string;
    readonly size: string;

    constructor({
      className,
      downloadDirectoryUrl,
      generalStats,
      httpClient,
      relatedClasses,
      lookupFileUrl,
      pldStatsFileUrl,
      sampleDataFileUrl,
      size,
    }: {
      className: string;
      downloadDirectoryUrl: string;
      generalStats: {
        hosts: number;
        quads: number;
        urls: number;
      };
      httpClient: HttpClient;
      lookupFileUrl: string;
      pldStatsFileUrl: string;
      relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
      sampleDataFileUrl: string;
      size: string;
    }) {
      this.className = className;
      this.downloadDirectoryUrl = downloadDirectoryUrl;
      this.generalStats = generalStats;
      this.httpClient = httpClient;
      this.lookupFileUrl = lookupFileUrl;
      this.pldStatsFileUrl = pldStatsFileUrl;
      this.relatedClasses = relatedClasses;
      this.sampleDataFileUrl = sampleDataFileUrl;
      this.size = size;
    }

    private async lookupCsvString(): Promise<string> {
      return (
        await streamToBuffer(await this.httpClient.get(this.lookupFileUrl))
      ).toString("utf8");
    }

    private async pldDataFileNames(): Promise<Record<string, string>> {
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
    async payLevelDomainSubsetsByDomain(): Promise<
      Record<string, SchemaDotOrgDataSet.ClassSubset.PayLevelDomainSubset>
    > {
      switch (this.className) {
        case "CreativeWork":
        case "LocalBusiness":
        case "Organization":
        case "Person":
        case "Product":
          // Skip large PLD stats files
          return {};
      }

      const pldDataFileNames = await this.pldDataFileNames();
      return (
        Papa.parse(await this.pldStatsCsvString(), {
          delimiter: "\t",
          header: true,
        }).data as any[]
      ).reduce(
        (
          map: Record<
            string,
            SchemaDotOrgDataSet.ClassSubset.PayLevelDomainSubset
          >,
          row: any
        ) => {
          const domain = row["Domain"] as string;
          if (domain.length === 0) {
            return map;
          }
          const dataFileName = pldDataFileNames[domain];
          invariant(dataFileName, "missing domain file: " + domain);

          map[domain] =
            new SchemaDotOrgDataSet.ClassSubset.PayLevelDomainSubset({
              dataFileUrl: this.downloadDirectoryUrl + "/" + dataFileName,
              domain,
              httpClient: this.httpClient,
              stats: {
                entitiesOfClass: parseInt(row["#Entities of class"]),
                propertiesAndDensity: parsePldStatsPropertiesAndDensity(
                  row["Properties and Density"]
                ),
                quadsOfSubset: parseInt(row["#Quads of Subset"]),
              },
            });
          return map;
        },
        {}
      );
    }

    private async pldStatsCsvString(): Promise<string> {
      return (
        await streamToBuffer(await this.httpClient.get(this.pldStatsFileUrl))
      ).toString("utf8");
    }

    private async sampleNquadsString(): Promise<string> {
      return (
        await streamToBuffer(await this.httpClient.get(this.sampleDataFileUrl))
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
      private readonly dataFileUrl: string;
      private readonly httpClient: HttpClient;
      readonly domain: string;
      readonly stats: PayLevelDomainSubset.Stats;

      constructor({
        dataFileUrl,
        domain,
        httpClient,
        stats,
      }: {
        dataFileUrl: string;
        domain: string;
        httpClient: HttpClient;
        stats: PayLevelDomainSubset.Stats;
      }) {
        this.dataFileUrl = dataFileUrl;
        this.domain = domain;
        this.httpClient = httpClient;
        this.stats = stats;
      }

      @Memoize()
      async dataset(): Promise<DatasetCore> {
        const gzStream = await this.httpClient.get(this.dataFileUrl);
        return new Promise((resolve, reject) => {
          const inflate = zlib.createInflate();
          const parser = new StreamParser();
          const quadStream = gzStream.pipe(inflate).pipe(parser);

          const store = new Store();
          quadStream.on("data", (data) => {
            console.info("data", data);
          });
          quadStream.on("end", (error: any) => {
            if (error) {
              reject(error);
            } else {
              resolve(store);
            }
          });
        });
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
