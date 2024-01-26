import ImmutableCache from "./ImmutableCache.js";
import HttpClient from "./HttpClient.js";
import SchemaDotOrgDataSet from "./SchemaDotOrgDataSet.js";

export default class WebDataCommons {
  private readonly cache: ImmutableCache;
  private readonly httpClient: HttpClient;
  private readonly showProgress: boolean;

  constructor({
    cacheDirectoryPath,
    readonly,
    showProgress,
  }: {
    cacheDirectoryPath: string;
    readonly?: boolean;
    showProgress?: boolean;
  }) {
    this.cache = new ImmutableCache({
      readonly,
      rootDirectoryPath: cacheDirectoryPath,
    });
    this.httpClient = new HttpClient({
      cache: this.cache,
      options: {
        retry: {
          limit: 10,
        },
        readonly,
        showProgress,
      },
    });
    this.showProgress = !!showProgress;
  }

  schemaDotOrgDataSet(kwds?: {version?: string}): SchemaDotOrgDataSet {
    return new SchemaDotOrgDataSet({
      cache: this.cache,
      httpClient: this.httpClient,
      showProgress: this.showProgress,
      version: kwds?.version ?? "2022-12",
    });
  }
}
