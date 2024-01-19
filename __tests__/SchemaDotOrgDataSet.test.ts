import SchemaDotOrgDataSet from "../src/SchemaDotOrgDataSet.js";
import WebDataCommons from "../src/WebDataCommons.js";
import cacheDirectoryPath from "./cacheDirectoryPath.js";

describe("SchemaDotOrgDataSet", () => {
  const schemaDotOrgDataSet = new WebDataCommons({
    cacheDirectoryPath,
  }).schemaDotOrgDataSet({});
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
    const pldStats =
      await administrativeAreaClassSubset.payLevelDomainSubsets();
    expect(pldStats.length).toBeGreaterThan(0);
    for (const pldStatsRow of pldStats) {
      expect(pldStatsRow.domain).not.toBe("");
      expect(pldStatsRow.stats.entitiesOfClass).toBeGreaterThan(0);
      expect(pldStatsRow.stats.quadsOfSubset).toBeGreaterThan(0);
    }
  });

  it("gets sample pages", async () => {
    const samplePagesByIri =
      await administrativeAreaClassSubset.samplePagesByIri();
    expect(Object.keys(samplePagesByIri)).toHaveLength(1);
  });

  it("gets empty PLD stats for Organization (large PLD stats file)", async () => {
    const sut = classSubsets.find(
      (classSubset) => classSubset.className === "Organization"
    )!;
    const pldStats =
      await administrativeAreaClassSubset.payLevelDomainSubsets();
    expect(pldStats).toHaveLength(0);
  });
});
