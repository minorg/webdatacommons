# webdatacommons

TypeScript library and CLI for working with Web Data Commons datasets

## Installation

    npm i webdatacommons

## Usage

```typescript
import WebDataCommons from "webdatacommons";

const classSpecificSubets = new WebDataCommons({cacheDirectoryPath: "path/to/directory"}).schemaDotOrgDataSet();
```
