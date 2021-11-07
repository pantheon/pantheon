import { initialState, queryEditorReducer } from "./QueryEditor.reducer";

describe("QueryEditor - main reducer", () => {
  it("should forward the action to aggregate reducer", () => {
    expect(initialState.aggregate.mode).toBe("visual");
    const state = queryEditorReducer(initialState, { type: "[aggregate] switchMode", payload: "code" });
    expect(state.aggregate.mode).toEqual("code");
    expect(state.queryType).toEqual("Aggregate");
  });

  it("should forward the action to record reducer", () => {
    expect(initialState.aggregate.mode).toBe("visual");
    const state = queryEditorReducer(initialState, { type: "[record] switchMode", payload: "code" });
    expect(state.aggregate.mode).toEqual("visual");
    expect(state.record.mode).toEqual("code");
    expect(state.queryType).toEqual("Aggregate");
  });

  it("should update the queryType", () => {
    const state = queryEditorReducer(initialState, { type: "updateQueryType", payload: "Record" });
    expect(state.queryType).toEqual("Record");
  });

  it("should update the sql query", () => {
    const state = queryEditorReducer(initialState, { type: "[sql] updateQuery", payload: "select * from store" });
    expect(state.sql).toEqual("select * from store");
  });
});
