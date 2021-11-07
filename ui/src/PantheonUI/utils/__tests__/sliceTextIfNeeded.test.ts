import { sliceTextIfNeeded } from "../sliceTextIfNeeded";

describe("sliceTextIfNeeded", () => {
  it("Should shorten long text to 30 chars default", () => {
    expect(
      sliceTextIfNeeded(
        "BlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlahBlah",
      ),
    ).toEqual("BlahBlahBlahBlahBlahBlahBlahBl…");
  });
  it("Should shorten long text to a configurable number of chars", () => {
    expect(sliceTextIfNeeded("BlahBlahBlahBlahBlahBlahBlahBlahBlahBlah", 3)).toEqual("Bla…");
  });
  it("Should return an empty string if no text given", () => {
    expect(sliceTextIfNeeded("")).toEqual("");
  });
  it("Should leave short text alone", () => {
    expect(sliceTextIfNeeded("Blah")).toEqual("Blah");
  });
});
