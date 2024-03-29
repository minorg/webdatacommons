/* eslint-disable @typescript-eslint/no-namespace */
import {
  NodeType,
  HTMLElement,
  TextNode,
  parse as parseHtml,
} from "node-html-parser";
import {Memoize} from "typescript-memoize";
import HttpClient from "./HttpClient.js";
import {Store, Quad, Parser, Writer} from "n3";
import Papa from "papaparse";
import {DatasetCore, NamedNode} from "@rdfjs/types";
import {invariant} from "ts-invariant";
import zlib from "node:zlib";
import streamToBuffer from "./streamToBuffer.js";
import {Readable} from "node:stream";
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

async function* parseNQuadsStream(stream: Readable) {
  const lineStream = stream
    // Have to parse the N-Quads line-by-line because the N3StreamParser gives up
    // on the first error instead of simply going to the next line.
    .pipe(split2());

  const parser = new Parser({format: "N-Quads"});

  for await (const line of lineStream) {
    let quads: Quad[];
    try {
      quads = parser.parse(line);
    } catch (error) {
      logger.trace("error parsing N-Quads stream: %s", error);
      continue;
    }
    invariant(quads.length === 1, "expected one quad per line");
    const quad = quads[0];

    yield quad;
  }
}

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
        const sizeCellTextParts = sizeCell.text.split(" ");
        const numberOfFiles = parseInt(
          sizeCellTextParts[sizeCellTextParts.length - 1].substring(
            1,
            sizeCellTextParts[sizeCellTextParts.length - 1].length - 1
          )
        );

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
          numberOfFiles,
          parent: this,
          pldStatsFileUrl,
          relatedClasses,
          sampleDataFileUrl: downloadHrefs[1],
          showProgress: this.showProgress,
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
    private readonly lookupFileUrl: string;
    readonly numberOfFiles: number;
    readonly parent: SchemaDotOrgDataSet;
    private readonly pldStatsFileUrl: string;
    readonly relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
    private readonly sampleDataFileUrl: string;
    private readonly showProgress: boolean;

    constructor({
      cache,
      className,
      downloadDirectoryUrl,
      generalStats,
      httpClient,
      lookupFileUrl,
      numberOfFiles,
      parent,
      pldStatsFileUrl,
      relatedClasses,
      sampleDataFileUrl,
      showProgress,
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
      numberOfFiles: number;
      parent: SchemaDotOrgDataSet;
      pldStatsFileUrl: string;
      relatedClasses: readonly SchemaDotOrgDataSet.ClassSubset.RelatedClass[];
      sampleDataFileUrl: string;
      showProgress: boolean;
    }) {
      this.cache = cache;
      this.className = className;
      this.downloadDirectoryUrl = downloadDirectoryUrl;
      this.generalStats = generalStats;
      this.httpClient = httpClient;
      this.lookupFileUrl = lookupFileUrl;
      this.numberOfFiles = numberOfFiles;
      this.parent = parent;
      this.pldStatsFileUrl = pldStatsFileUrl;
      this.relatedClasses = relatedClasses;
      this.sampleDataFileUrl = sampleDataFileUrl;
      this.showProgress = showProgress;
    }

    async *dataset() {
      for (let fileI = 0; fileI < this.numberOfFiles; fileI++) {
        for await (const quad of parseNQuadsStream(
          (
            await this.httpClient.get(
              `${this.downloadDirectoryUrl}/part_${fileI}.gz`
            )
          ).pipe(zlib.createGunzip())
        )) {
          yield quad;
        }
      }
    }

    @Memoize()
    async payLevelDomainSubsets(): Promise<
      readonly SchemaDotOrgDataSet.ClassSubset.PayLevelDomainSubset[]
    > {
      return Object.values(await this.payLevelDomainSubsetsByDomain());
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

      let pldDataFileNames: Record<string, string>;
      try {
        pldDataFileNames = Papa.parse(
          (
            await streamToBuffer(await this.httpClient.get(this.lookupFileUrl))
          ).toString("utf8"),
          {
            header: true,
          }
        ).data.reduce((map: Record<string, string>, row: any) => {
          if (row["pld"].length > 0) {
            map[row["pld"]] = row["file_lookup"];
          }
          return map;
        }, {});
      } catch (e) {
        // The 2022-12 Painting lookup returns 403
        logger.error("error getting and parsing %s: %s", this.lookupFileUrl, e);
        return {};
      }

      return (
        Papa.parse(
          (
            await streamToBuffer(
              await this.httpClient.get(this.pldStatsFileUrl)
            )
          ).toString("utf8"),
          {
            delimiter: "\t",
            header: true,
          }
        ).data as any[]
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

    @Memoize()
    async samplePages(): Promise<
      readonly SchemaDotOrgDataSet.ClassSubset.PageSubset[]
    > {
      return Object.values(await this.samplePagesByIri());
    }

    @Memoize()
    async samplePagesByIri(): Promise<
      Record<string, SchemaDotOrgDataSet.ClassSubset.PageSubset>
    > {
      const result: Record<string, SchemaDotOrgDataSet.ClassSubset.PageSubset> =
        {};
      for await (const quad of parseNQuadsStream(
        await this.httpClient.get(this.sampleDataFileUrl)
      )) {
        const pageIri = quad.graph;
        if (pageIri.termType !== "NamedNode") {
          continue;
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
      }
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

      async *dataset() {
        const cacheKey = this.datasetCacheKey(this.domain, {compressed: true});

        let cacheFileStream = await this.cache.get(cacheKey);
        if (cacheFileStream == null) {
          await this.getAndSplitDataFile();
          cacheFileStream = await this.cache.get(cacheKey);
          invariant(
            cacheFileStream !== null,
            `${this.domain} not present in data file`
          );
        }

        const cacheFilePath = this.cache.filePath(cacheKey);
        logger.debug("decompressing and parsing %s", cacheFilePath);
        const progressBar = new cliProgress.SingleBar({
          format: `decompressing and parsing ${cacheFilePath}: {value} quads`,
          stream: this.showProgress ? process.stderr : devNull,
        });

        progressBar.start(Number.MAX_SAFE_INTEGER, 0);
        for await (const quad of parseNQuadsStream(
          cacheFileStream.pipe(zlib.createBrotliDecompress())
        )) {
          progressBar.increment();
          yield quad;
        }
        progressBar.stop();
      }

      /**
       * Get the cache key associated with a pay-level domain.
       */
      private datasetCacheKey(
        payLevelDomainName: string,
        {compressed}: {compressed: boolean}
      ): ImmutableCache.Key {
        return [
          "pld-datasets",
          this.parent.parent.version,
          this.parent.className,
          payLevelDomainName + ".nq" + (compressed ? ".br" : ""),
        ];
      }

      /**
       * Get and split the data file (e.g., "AdministrativeArea/part_0.gz") that contains this pay-level domain (PLD)'s data for the
       * associated schema.org class-specific subset.
       *
       * This method is not thread-safe.
       *
       * The process works as follows:
       * 1. Download the gzipped N-Quads file to the cache if necessary.
       * 2. Simultaneously uncompress and iterate over each line in the file. (See note below re: why this is line-by-line.)
       * 3. For each line, parse the N-Quad.
       * 4. Batch contiguous N-Quads belonging to a single PLD in memory.
       * 5. When a new pay-level domain name is seen, flush the current batch to an uncompressed .nq file, which serves as a temporary
       *  store for quads related to that PLD. There is one temporary .nq file per PLD.
       * 6. When all lines from the source N-Quads file have been seen, compress the temporary, uncompressed per-PLD files (.nq -> .nq.br).
       *   A compressed file is considered complete. Delete the temporary uncompressed (.nq) file.
       *
       * The result is a (cache) directory full of <PLD>.nq.br files for all PLDs represented in the data file.
       * The .datasetCached method reads the <PLD>.nq.br file corresponding to this PLD instance, but other PLD instances that refer to the same
       * data file will reuse their respective <PLD>.nq.br files from the cache without having to run the split themselves.
       */
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
        const payLevelDomainsProgressBar = progressBars.create(
          payLevelDomainNames.size,
          0
        );

        // Batch quads for a PLD in memory to eliminate some disk writes
        type Batch = {
          payLevelDomainName: string;
          quads: Quad[];
        };
        let batch: Batch | null = null;
        // One file per pay level domain name
        // Keep the file handles open in case the quads for a PLD are not contiguous
        const fileStreamsByPayLevelDomainName: Record<
          string,
          fs.WriteStream | null
        > = {};
        const writer = new Writer({format: "N-Quads"});

        const flushBatch = async (batch: Batch) => {
          invariant(batch.quads.length > 0);

          let fileStream =
            fileStreamsByPayLevelDomainName[batch.payLevelDomainName];
          if (fileStream === null) {
            // We set fileStream to null in the else branch to indicate that a
            // complete, compressed file for this pay-level domain already exists.
            logger.trace(
              "skipping write to already-complete pay-level domain: %s",
              batch.payLevelDomainName
            );
            return;
          } else if (fileStream != null) {
            // i.e., it's not undefined
            logger.trace(
              "reusing file stream for pay-level domain: %s",
              batch.payLevelDomainName
            );
          } else {
            payLevelDomainsProgressBar.increment({
              metric: "pay-level domains seen",
            });
            logger.trace(
              "data file %s: encountered new pay-level domain: %s",
              this.dataFileUrl,
              batch.payLevelDomainName
            );

            // A complete, compressed file already exists for this pay-level domain
            // Skip further writes.
            if (
              await this.cache.has(
                this.datasetCacheKey(batch.payLevelDomainName, {
                  compressed: true,
                })
              )
            ) {
              fileStreamsByPayLevelDomainName[batch.payLevelDomainName] = null;
              logger.trace(
                "pay-level domain %s is already complete, will skip further writes",
                batch.payLevelDomainName
              );
              return;
            }

            // Create a new uncompressed file for this pay-level domain.
            // This will zero out any existing data in the file.
            fileStream = await this.cache.createWriteStream(
              this.datasetCacheKey(batch.payLevelDomainName, {
                compressed: false,
              })
            );
            fileStreamsByPayLevelDomainName[batch.payLevelDomainName] =
              fileStream;
          }

          await new Promise<void>((resolve) => {
            fileStream!.write(
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
          for await (const quad of parseNQuadsStream(
            (await this.httpClient.get(this.dataFileUrl)).pipe(
              zlib.createGunzip()
            )
          )) {
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

          payLevelDomainsProgressBar.stop();

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
                        this.datasetCacheKey(payLevelDomainName, {
                          compressed: false,
                        })
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
          closeProgressBar.stop();
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
              (fileStream) =>
                new Promise((resolve) => {
                  if (fileStream) {
                    fileStream.end(resolve);
                  }
                })
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
