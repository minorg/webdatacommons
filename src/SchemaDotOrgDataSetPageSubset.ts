import {DatasetCore, NamedNode} from "@rdfjs/types";

export default class SchemaDotDataSetCorpusPageSubset {
  readonly dataset: DatasetCore;
  readonly pageIri: NamedNode;

  constructor({dataset, pageIri}: {dataset: DatasetCore; pageIri: NamedNode}) {
    this.dataset = dataset;
    this.pageIri = pageIri;
  }
}
