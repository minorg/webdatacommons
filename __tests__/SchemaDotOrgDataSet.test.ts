import SchemaDotOrgDataSet from "../src/SchemaDotOrgDataSet.js";
import WebDataCommons from "../src/WebDataCommons.js";
import cacheDirectoryPath from "./cacheDirectoryPath.js";
// import v8Profiler from "v8-profiler-next";
// import fs from "node:fs";

describe("SchemaDotOrgDataSet", () => {
  let classSubsets: readonly SchemaDotOrgDataSet.ClassSubset[];
  let administrativeAreaClassSubset: SchemaDotOrgDataSet.ClassSubset;

  beforeAll(async () => {
    classSubsets = await new WebDataCommons({cacheDirectoryPath})
      .schemaDotOrgDataSet({})
      .classSubsets();
    administrativeAreaClassSubset = classSubsets[0];
    expect(administrativeAreaClassSubset.className).toBe("AdministrativeArea");
  });

  it("gets the class-specific subsets", async () => {
    expect(classSubsets).toHaveLength(48);
    for (const classSubset of classSubsets) {
      expect(classSubset.className).not.toBe("");
      expect(classSubset.generalStats.hosts).toBeGreaterThan(0);
      expect(classSubset.generalStats.quads).toBeGreaterThan(0);
      expect(classSubset.generalStats.urls).toBeGreaterThan(0);
      expect(classSubset.relatedClasses).not.toHaveLength(0);
      expect(classSubset.size).not.toBe("");
    }
  });

  it("gets PLD stats", async () => {
    const pldSubsets = Object.values(
      await administrativeAreaClassSubset.payLevelDomainSubsetsByDomain()
    );
    expect(pldSubsets.length).toBeGreaterThan(0);
    for (const pldStatsRow of pldSubsets) {
      expect(pldStatsRow.domain).not.toBe("");
      expect(pldStatsRow.stats.entitiesOfClass).toBeGreaterThan(0);
      expect(pldStatsRow.stats.quadsOfSubset).toBeGreaterThan(0);
    }
  });

  it("gets empty PLD stats for Organization (large PLD stats file)", async () => {
    const organizationClassSubset = classSubsets.find(
      (classSubset) => classSubset.className === "Organization"
    )!;
    const pldStats = Object.values(
      await organizationClassSubset.payLevelDomainSubsetsByDomain()
    );
    expect(pldStats).toHaveLength(0);
  });

  it(
    "gets a PLD dataset",
    async () => {
      if (process.env.CI) {
        return;
      }

      // v8Profiler.setGenerateType(1);

      // const profileTitle = "get-pld-dataset";
      // v8Profiler.startProfiling(profileTitle, true);
      // setTimeout(
      //   () => {
      //     const profile = v8Profiler.stopProfiling(profileTitle);
      //     profile.export(function (error, result) {
      //       // if it doesn't have the extension .cpuprofile then
      //       // chrome's profiler tool won't like it.
      //       // examine the profile:
      //       //   Navigate to chrome://inspect
      //       //   Click Open dedicated DevTools for Node
      //       //   Select the profiler tab
      //       //   Load your file
      //       // @ts-expect-error
      //       fs.writeFileSync(`${profileTitle}.cpuprofile`, result);
      //       profile.delete();
      //     });
      //   },
      //   5 * 60 * 1000
      // );
      const pldSubset = (
        await administrativeAreaClassSubset.payLevelDomainSubsetsByDomain()
      )["balsamohomes.com"];
      expect(pldSubset).not.toBeUndefined();
      const dataset = await pldSubset.dataset();
      expect(dataset.size).toBeGreaterThan(0);
    },
    60 * 60 * 1000
  );

  it("gets sample pages", async () => {
    const samplePagesByIri =
      await administrativeAreaClassSubset.samplePagesByIri();
    expect(Object.keys(samplePagesByIri)).toHaveLength(1);
  });
});
