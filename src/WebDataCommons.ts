import HttpClient from "./HttpClient.js";
import SchemaDotOrgDataSet from "./SchemaDotOrgDataSet.js";

export default class WebDataCommons {
  private readonly httpClient: HttpClient;

  constructor({cacheDirectoryPath}: {cacheDirectoryPath: string}) {
    this.httpClient = new HttpClient({
      cacheDirectoryPath,
      gotOptions: {
        retry: {
          limit: 10,
        },
      },
    });
  }

  schemaDotOrgDataSet({version}: {version?: string}): SchemaDotOrgDataSet {
    return new SchemaDotOrgDataSet({
      httpClient: this.httpClient,
      version: version ?? "2022-12",
    });
  }
}
