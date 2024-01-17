import WebDataCommonsCorpus from "@/lib/models/WebDataCommonsCorpus";
import WebDataCommonsCorpusClassSpecificSubset from "@/lib/models/WebDataCommonsCorpusClassSpecificSubset";

describe("WebDataCommonsCorpusClassSpecificSubset", () => {
  let sut: WebDataCommonsCorpusClassSpecificSubset;

  beforeAll(async () => {
    sut = (await new WebDataCommonsCorpus({}).classSpecificSubsets())[0];
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

  it(
    "get all PLD stats files in parallel",
    async () => {
      await Promise.all(
        (await new WebDataCommonsCorpus({}).classSpecificSubsets()).map(
          (classSpecificSubset) => classSpecificSubset.pldStatsCsvString()
        )
      );
    },
    30 * 60 * 1000
  );

  it("gets empty PLD stats for Organization (large PLD stats file)", async () => {
    const sut = (
      await new WebDataCommonsCorpus({}).classSpecificSubsets()
    ).find(
      (classSpecificSubset) => classSpecificSubset.className === "Organization"
    )!;
    const pldStats = await sut.pldStats();
    expect(pldStats).toHaveLength(0);
  });

  it(
    "get all sample files in parallel",
    async () => {
      await Promise.all(
        (await new WebDataCommonsCorpus({}).classSpecificSubsets()).map(
          (classSpecificSubset) => classSpecificSubset.sampleNquadsString()
        )
      );
    },
    30 * 60 * 1000
  );
});
