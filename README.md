# webdatacommons

TypeScript library for Node 16+ and command line interface for working with Web Data Commons datasets

## Installation

    npm i webdatacommons

## Usage

### From JavaScript/TypeScript

```typescript
import WebDataCommons from "webdatacommons";

const classSubsets = await new WebDataCommons({cacheDirectoryPath: "path/to/directory"}).schemaDotOrgDataSet().classSubsets();
```

### From the command line

Extract quads for the pay-level domain `balsamohomes.com` from the `AdministrativeArea` class-specific subset and print them to the console:

```bash
    webdatacommons schema.org extract -c AdministrativeArea -d balsamohomes.com
```
