import * as urlHelpers from "../urlHelpers";

test("removes query string from URL", () => {
  expect(urlHelpers.removeQuery("/url?a=b")).toEqual("/url");
});

test("does not remove query string from URL if one isn't present", () => {
  expect(urlHelpers.removeQuery("/url2")).toEqual("/url2");
});
