import { createAggregateFakeQuery } from "./aggregate.reducer.test";
import { initialState, QueryEditorState } from "./QueryEditor.reducer";
import { getHistory } from "./QueryEditor.selector";
import { createRecordFakeQuery } from "./record.reducer.test";

describe("QueryEditor - selector", () => {
  describe("getHistory", () => {
    it("should return undefined for Sql Query", () => {
      const state: QueryEditorState = { ...initialState, queryType: "Sql" };
      const history = getHistory(state);
      expect(history).toBeUndefined();
    });

    describe("with aggregate query", () => {
      it("should return not be clearable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Aggregate" };
        const history = getHistory(state);
        expect(history.canClear).toBeFalsy();
      });

      it("should return not be undoable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Aggregate" };
        const history = getHistory(state);
        expect(history.canUndo).toBeFalsy();
      });

      it("should return not be redoable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Aggregate" };
        const history = getHistory(state);
        expect(history.canUndo).toBeFalsy();
      });

      it("should be clearable if I have a query set", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Aggregate",
          aggregate: { ...initialState.aggregate, query: createAggregateFakeQuery("test") },
        };
        const history = getHistory(state);
        expect(history.canClear).toBeTruthy();
      });

      it("should be undoable if I have some previous queries", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Aggregate",
          aggregate: { ...initialState.aggregate, previousQueries: [createAggregateFakeQuery("test")] },
        };
        const history = getHistory(state);
        expect(history.canUndo).toBeTruthy();
      });

      it("should be redoable if I have some next queries", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Aggregate",
          aggregate: { ...initialState.aggregate, nextQueries: [createAggregateFakeQuery("test")] },
        };
        const history = getHistory(state);
        expect(history.canRedo).toBeTruthy();
      });

      it("should return all aggretate actions", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Aggregate" };
        const history = getHistory(state);
        expect(history.clearAction).toEqual({ type: "[aggregate] clearQuery" });
        expect(history.undoAction).toEqual({ type: "[aggregate] undo" });
        expect(history.redoAction).toEqual({ type: "[aggregate] redo" });
      });
    });

    describe("with record query", () => {
      it("should return not be clearable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Record" };
        const history = getHistory(state);
        expect(history.canClear).toBeFalsy();
      });

      it("should return not be undoable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Record" };
        const history = getHistory(state);
        expect(history.canUndo).toBeFalsy();
      });

      it("should return not be redoable with an empty history", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Record" };
        const history = getHistory(state);
        expect(history.canUndo).toBeFalsy();
      });

      it("should be clearable if I have a query set", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Record",
          record: { ...initialState.record, query: createRecordFakeQuery("test") },
        };
        const history = getHistory(state);
        expect(history.canClear).toBeTruthy();
      });

      it("should be undoable if I have some previous queries", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Record",
          record: { ...initialState.record, previousQueries: [createRecordFakeQuery("test")] },
        };
        const history = getHistory(state);
        expect(history.canUndo).toBeTruthy();
      });

      it("should be redoable if I have some next queries", () => {
        const state: QueryEditorState = {
          ...initialState,
          queryType: "Record",
          record: { ...initialState.record, nextQueries: [createRecordFakeQuery("test")] },
        };
        const history = getHistory(state);
        expect(history.canRedo).toBeTruthy();
      });

      it("should return all aggretate actions", () => {
        const state: QueryEditorState = { ...initialState, queryType: "Record" };
        const history = getHistory(state);
        expect(history.clearAction).toEqual({ type: "[record] clearQuery" });
        expect(history.undoAction).toEqual({ type: "[record] undo" });
        expect(history.redoAction).toEqual({ type: "[record] redo" });
      });
    });
  });
});
