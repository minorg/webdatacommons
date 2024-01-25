import ImmutableCache from "./ImmutableCache.js";
import HttpClient from "./HttpClient.js";
import SchemaDotOrgDataSet from "./SchemaDotOrgDataSet.js";

export default class WebDataCommons {
  private readonly cache: ImmutableCache;
  private readonly httpClient: HttpClient;

  constructor({cacheDirectoryPath}: {cacheDirectoryPath: string}) {
    this.cache = new ImmutableCache({rootDirectoryPath: cacheDirectoryPath});
    this.httpClient = new HttpClient({
      cache: this.cache,
      gotOptions: {
        retry: {
          limit: 10,
        },
      },
    });
  }

  schemaDotOrgDataSet(kwds?: {version?: string}): SchemaDotOrgDataSet {
    return new SchemaDotOrgDataSet({
      cache: this.cache,
      httpClient: this.httpClient,
      version: kwds?.version ?? "2022-12",
    });
  }
}
