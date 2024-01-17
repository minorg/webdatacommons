import SchemaDotOrgDataSetClassSpecificSubset from "../src/SchemaDotOrgDataSetClassSpecificSubset.js";
import WebDataCommons from "../src/WebDataCommons.js";
import cacheDirectoryPath from "./cacheDirectoryPath.js";

describe("SchemaDotOrgDataSetClassSpecificSubset", () => {
  let classSpecificSubsets: readonly SchemaDotOrgDataSetClassSpecificSubset[];
  let sut: SchemaDotOrgDataSetClassSpecificSubset;

  beforeAll(async () => {
    classSpecificSubsets = await new WebDataCommons({cacheDirectoryPath})
      .schemaDotOrgDataSet({})
      .classSpecificSubsets();
    sut = classSpecificSubsets[0];
    expect(sut.className).toBe("AdministrativeArea");
  });

  it("gets PLD stats", async () => {
    const pldStats = await sut.pldStats();
    expect(pldStats.length).toBeGreaterThan(0);
    for (const pldStatsRow of pldStats) {
      expect(pldStatsRow.domain).not.toBe("");
      expect(pldStatsRow.entitiesOfClass).toBeGreaterThan(0);
      expect(pldStatsRow.quadsOfSubset).toBeGreaterThan(0);
    }
  });

  it("gets sample pages", async () => {
    const samplePagesByIri = await sut.samplePagesByIri();
    expect(Object.keys(samplePagesByIri)).toHaveLength(1);
  });

  it.skip(
    "get all PLD stats files in parallel",
    async () => {
      await Promise.all(
        classSpecificSubsets.map((classSpecificSubset) =>
          classSpecificSubset.pldStatsCsvString()
        )
      );
    },
    30 * 60 * 1000
  );

  it("gets empty PLD stats for Organization (large PLD stats file)", async () => {
    const sut = classSpecificSubsets.find(
      (classSpecificSubset) => classSpecificSubset.className === "Organization"
    )!;
    const pldStats = await sut.pldStats();
    expect(pldStats).toHaveLength(0);
  });

  it.skip(
    "get all sample files in parallel",
    async () => {
      await Promise.all(
        classSpecificSubsets.map((classSpecificSubset) =>
          classSpecificSubset.sampleNquadsString()
        )
      );
    },
    30 * 60 * 1000
  );
});
