/* eslint-disable @typescript-eslint/no-namespace */
import {
  NodeType,
  HTMLElement,
  TextNode,
  parse as parseHtml,
} from "node-html-parser";
import {Memoize} from "typescript-memoize";
import HttpClient from "./HttpClient.js";
import {StreamParser, Store, Quad, Parser, Writer} from "n3";
import Papa from "papaparse";
import {DatasetCore, NamedNode} from "@rdfjs/types";
import {invariant} from "ts-invariant";
import zlib from "node:zlib";
import streamToBuffer from "./streamToBuffer.js";
import {Stream} from "node:stream";
import cliProgress from "cli-progress";
import logger from "./logger.js";
import ImmutableCache from "./ImmutableCache.js";
import split2 from "split2";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import brotliCompressTextFile from "./brotliCompressTextFile.js";
// @ts-expect-error No types
import devNull from "dev-null";

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
  private readonly cache: ImmutableCache;
  private readonly httpClient: HttpClient;
  private readonly showProgress: boolean;
  readonly version: string;

  constructor({
    cache,
    httpClient,
    showProgress,
    version,
  }: {
    cache: ImmutableCache;
    httpClient: HttpClient;
    showProgress: boolean;
    version: string;
  }) {
    this.cache = cache;
    this.httpClient = httpClient;
    this.showProgress = showProgress;
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
          cache: this.cache,
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
          showProgress: this.showProgress,
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
    private readonly cache: ImmutableCache;
    readonly className: string;
    private readonly downloadDirectoryUrl: string;
    readonly generalStats: SchemaDotOrgDataSet.ClassSubset.GeneralStats;
    private readonly httpClient: HttpClient;
    private readonly pldStatsFileUrl: string;
    private readonly lookupFileUrl: string;
    readonly relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
    private readonly sampleDataFileUrl: string;
    private readonly showProgress: boolean;
    readonly size: string;

    constructor({
      cache,
      className,
      downloadDirectoryUrl,
      generalStats,
      httpClient,
      lookupFileUrl,
      pldStatsFileUrl,
      relatedClasses,
      sampleDataFileUrl,
      showProgress,
      size,
    }: {
      cache: ImmutableCache;
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
      showProgress: boolean;
      size: string;
    }) {
      this.cache = cache;
      this.className = className;
      this.downloadDirectoryUrl = downloadDirectoryUrl;
      this.generalStats = generalStats;
      this.httpClient = httpClient;
      this.lookupFileUrl = lookupFileUrl;
      this.pldStatsFileUrl = pldStatsFileUrl;
      this.relatedClasses = relatedClasses;
      this.sampleDataFileUrl = sampleDataFileUrl;
      this.showProgress = showProgress;
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
              cache: this.cache,
              dataFileName,
              dataFileUrl: this.downloadDirectoryUrl + "/" + dataFileName,
              domain,
              httpClient: this.httpClient,
              parent: this,
              showProgress: this.showProgress,
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

    private async sampleNquadsStream(): Promise<Stream> {
      return await this.httpClient.get(this.sampleDataFileUrl);
    }

    @Memoize()
    async samplePagesByIri(): Promise<
      Record<string, SchemaDotOrgDataSet.ClassSubset.PageSubset>
    > {
      const quadStream = (await this.sampleNquadsStream()).pipe(
        new StreamParser({format: "N-Quads"})
      );
      return new Promise((resolve, reject) => {
        const result: Record<
          string,
          SchemaDotOrgDataSet.ClassSubset.PageSubset
        > = {};
        quadStream.on("data", (quad) => {
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
        quadStream.on("error", (error) => {
          logger.error("error parsing %s: %s", this.sampleDataFileUrl, error);
        });
        quadStream.on("end", (error: any) => {
          if (error) {
            reject(error);
          } else {
            resolve(result);
          }
        });
      });
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
      private readonly cache: ImmutableCache;
      private readonly dataFileName: string;
      private readonly dataFileUrl: string;
      private readonly httpClient: HttpClient;
      private readonly parent: SchemaDotOrgDataSet.ClassSubset;
      readonly domain: string;
      private readonly showProgress: boolean;
      readonly stats: PayLevelDomainSubset.Stats;

      constructor({
        cache,
        dataFileName,
        dataFileUrl,
        domain,
        httpClient,
        parent,
        showProgress,
        stats,
      }: {
        cache: ImmutableCache;
        dataFileName: string;
        dataFileUrl: string;
        domain: string;
        httpClient: HttpClient;
        parent: SchemaDotOrgDataSet.ClassSubset;
        showProgress: boolean;
        stats: PayLevelDomainSubset.Stats;
      }) {
        this.cache = cache;
        this.dataFileName = dataFileName;
        this.dataFileUrl = dataFileUrl;
        this.domain = domain;
        this.httpClient = httpClient;
        this.parent = parent;
        this.showProgress = showProgress;
        this.stats = stats;
      }

      @Memoize()
      async dataset(): Promise<DatasetCore> {
        const dataset = await this.datasetCached();
        if (dataset !== null) {
          return dataset;
        }

        await this.getAndSplitDataFile();

        {
          const dataset = await this.datasetCached();
          if (dataset === null) {
            throw new RangeError(`unable to get ${this.domain} dataset`);
          }
          return dataset;
        }
      }

      private datasetCacheKey(
        domain: string,
        fileNameSuffix?: string
      ): ImmutableCache.Key {
        return [
          "pld-datasets",
          this.parent.className,
          domain + ".nq" + (fileNameSuffix ?? ""),
        ];
      }

      private async datasetCached(): Promise<DatasetCore | null> {
        // The dataset is only considered cached if the compressed version exists,
        // because that indicates the source data (part_X.gz) was processed completely.
        const cacheKey = this.datasetCacheKey(this.domain, ".br");
        const cacheFileStream = await this.cache.get(cacheKey);
        if (cacheFileStream === null) {
          return null;
        }
        const quadStream = cacheFileStream
          .pipe(zlib.createBrotliDecompress())
          .pipe(new StreamParser({format: "N-Quads"}));
        return new Promise((resolve, reject) => {
          const store = new Store();
          quadStream.on("data", (quad) => {
            store.add(quad);
          });
          quadStream.on("error", (error) => {
            logger.error("error parsing %s: %s", cacheKey, error);
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

      private async getAndSplitDataFile(): Promise<void> {
        // Download the data file and split it into one cached file per PLD.
        const payLevelDomainNames = new Set(
          Object.keys(await this.parent.payLevelDomainSubsetsByDomain())
        );
        const lookupPayLevelDomainName = (hostname: string): string | null => {
          // We know the universe of pay-level domains that are supposed to be in this data file.
          // Rather than parsing the pay-level domain from the URL hostname,
          // which involves Public Suffix List lookups,
          // go through the universe of PLDs we expect to find.
          for (let i = 0; i < hostname.length; i++) {
            const hostnameSuffix = hostname.substring(i);
            if (payLevelDomainNames.has(hostnameSuffix)) {
              return hostnameSuffix;
            }
          }
          return null;
        };

        const progressBars = new cliProgress.MultiBar({
          format: `Split ${this.parent.className} ${this.dataFileName} {metric} [{bar}] {percentage}% | {value}/{total}`,
          stream: this.showProgress ? process.stderr : devNull,
        });
        const pldsProgressBar = progressBars.create(
          payLevelDomainNames.size,
          0
        );

        const lineStream = (await this.httpClient.get(this.dataFileUrl))
          .pipe(zlib.createGunzip())
          // Have to parse the N-Quads line-by-line because the N3StreamParser gives up
          // on the first error instead of simply going to the next line.
          .pipe(split2());

        // Batch quads for a PLD in memory to eliminate some disk writes
        type Batch = {
          payLevelDomainName: string;
          quads: Quad[];
        };
        let batch: Batch | null = null;
        // One file per pay level domain name
        // Keep the file handles open in case the quads for a PLD are not contiguous
        const fileStreamsByPayLevelDomainName: Record<string, fs.WriteStream> =
          {};
        const parser = new Parser({format: "N-Quads"});
        const writer = new Writer({format: "N-Quads"});

        const flushBatch = async (batch: Batch) => {
          invariant(batch.quads.length > 0);

          let fileStream =
            fileStreamsByPayLevelDomainName[batch.payLevelDomainName];
          if (fileStream) {
            logger.trace(
              "reusing file handle for pay-level domain: %s",
              batch.payLevelDomainName
            );
          } else {
            fileStream = await this.cache.createWriteStream(
              this.datasetCacheKey(batch.payLevelDomainName)
            );
            fileStreamsByPayLevelDomainName[batch.payLevelDomainName] =
              fileStream;

            pldsProgressBar.increment({metric: "pay-level domains seen"});
            logger.trace(
              "data file %s: encountered new pay-level domain: %s",
              this.dataFileUrl,
              batch.payLevelDomainName
            );
          }

          await new Promise<void>((resolve) => {
            fileStream.write(
              batch.quads
                .map((quad) =>
                  writer.quadToString(
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    quad.graph
                  )
                )
                .join(""),
              () => resolve()
              // () => fileStream.flush(resolve)
            );
          });
        };

        try {
          for await (const line of lineStream) {
            let quads: Quad[];
            try {
              quads = parser.parse(line);
            } catch (error) {
              logger.trace(
                "error parsing data file %s: %s",
                this.dataFileUrl,
                error
              );
              continue;
            }
            invariant(quads.length === 1, "expected one quad per line");
            const quad = quads[0];

            if (quad.graph.termType !== "NamedNode") {
              logger.warn("non-IRI quad graph: ", quad.graph.value);
              continue;
            }

            let payLevelDomainName: string;
            if (
              batch !== null &&
              quad.graph.value ===
                batch.quads[batch.quads.length - 1].graph.value
            ) {
              // Fast path:
              // If the quad's graph is the same as the last quad's graph,
              // reuse the payLevelDomainName instead of parsing it.
              payLevelDomainName = batch.payLevelDomainName;
            } else {
              // Slow path:
              // New graph. Figure out the payLevelDomainName.

              let url: URL;
              try {
                url = new URL(quad.graph.value);
              } catch {
                logger.warn("non-URL IRI: %s", quad.graph.value);
                continue;
              }

              const payLevelDomainNameLookup = lookupPayLevelDomainName(
                url.hostname
              );
              if (payLevelDomainNameLookup === null) {
                logger.warn(
                  "no known pay-level domain name in URL: %s",
                  quad.graph.value
                );
                continue;
              }
              payLevelDomainName = payLevelDomainNameLookup;
            }

            // Add the quad to the batch
            if (batch === null) {
              batch = {payLevelDomainName, quads: [quad]};
            } else if (batch.payLevelDomainName === payLevelDomainName) {
              batch.quads.push(quad);
            } else {
              // Flush the batch and start a new batch
              await flushBatch(batch);
              batch = {payLevelDomainName, quads: [quad]};
            }
          }

          if (batch !== null) {
            await flushBatch(batch);
          }

          // Finished successfully.
          // Close all open files, compress them, and delete the uncompressed versions.
          // The file/dataset is only considered cached if the compressed version exists.
          logger.info(
            "closing and compressing %d files",
            payLevelDomainNames.size
          );
          const closeProgressBar = progressBars.create(
            payLevelDomainNames.size,
            0
          );
          await Promise.all(
            [...payLevelDomainNames].map(
              (payLevelDomainName) =>
                new Promise<void>((resolve, reject) => {
                  const fileStream =
                    fileStreamsByPayLevelDomainName[payLevelDomainName];
                  if (fileStream) {
                    fileStream.end(() => {
                      const uncompressedFilePath = this.cache.filePath(
                        this.datasetCacheKey(payLevelDomainName)
                      );
                      brotliCompressTextFile(uncompressedFilePath).then(
                        () =>
                          fsPromises.unlink(uncompressedFilePath).then(() => {
                            closeProgressBar.increment({
                              metric: "files closed and compressed",
                            });
                            resolve();
                          }, reject),
                        reject
                      );
                    });
                  } else {
                    logger.error(
                      "pay-level domain %s not represented in data file %s",
                      payLevelDomainName,
                      this.dataFileUrl
                    );
                  }
                })
            )
          );
          logger.info(
            "closed and compressed %d files",
            Object.keys(fileStreamsByPayLevelDomainName).length
          );
        } catch (error) {
          logger.error(
            "error processing data file %s: %s",
            this.dataFileUrl,
            error
          );

          // Close all open files
          const fileStreams = Object.values(fileStreamsByPayLevelDomainName);
          logger.debug("closing %d files", fileStreams.length);
          await Promise.all(
            fileStreams.map(
              (fileStream) => new Promise((resolve) => fileStream.end(resolve))
            )
          );
          logger.debug("closed %d files", fileStreams.length);
        }
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
