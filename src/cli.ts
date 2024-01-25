import WebDataCommons from "./WebDataCommons.js";
import {
  binary,
  command,
  option,
  optional,
  string,
  subcommands,
  run,
} from "cmd-ts";
import {Writer} from "n3";
import globalCacheDir from "global-cache-dir";

const defaultCacheDirectoryPath = await globalCacheDir("webdatacommons");

const extractSchemaDotOrgCommand = command({
  args: {
    cacheDirectoryPath: option({
      defaultValue: () => defaultCacheDirectoryPath,
      long: "cache-directory-path",
      type: string,
    }),
    class: option({
      long: "class",
      description: "schema.org class/type name, such as AdministrativeArea",
      short: "c",
      type: string,
    }),
    dataSetVersion: option({
      long: "data-set-version",
      description: "version of the Schema.org data set e.g., 2022-12",
      type: optional(string),
    }),
    format: option({
      defaultValue: () => "application/trig",
      description: "RDF format, of those supported by N3.js",
      long: "format",
      short: "f",
    }),
    pld: option({
      description:
        "pay-level domain/top private domain, such as balsamohomes.com",
      long: "pay-level-domain",
      short: "d",
      type: string,
    }),
  },
  name: "extract",
  handler: async function (args) {
    const dataSet = new WebDataCommons({
      cacheDirectoryPath: args.cacheDirectoryPath,
    }).schemaDotOrgDataSet({version: args.dataSetVersion});

    const classSubset = (await dataSet.classSubsetsByClassName())[args.class];
    if (!classSubset) {
      throw new RangeError("unknown class: " + args.class);
    }

    const pldSubset = (await classSubset.payLevelDomainSubsetsByDomain())[
      args.pld
    ];
    if (!pldSubset) {
      throw new RangeError("unknown pay-level domain: " + args.pld);
    }

    const dataset = await pldSubset.dataset();

    const writer = new Writer(process.stdout, {format: args.format});
    for (const quad of dataset) {
      writer.addQuad(quad);
    }

    writer.end();
  },
});

run(
  binary(
    subcommands({
      name: "webdatacommons",
      cmds: {
        "schema.org": subcommands({
          name: "schema.org",
          cmds: {
            extract: extractSchemaDotOrgCommand,
          },
        }),
      },
    })
  ),
  process.argv
);
