import { initialRecordState, queryEditorRecordReducer, RecordQuery } from "./record.reducer";

export const createRecordFakeQuery = (id: string): RecordQuery => ({
  offset: 0,
  limit: 10,
  rows: [id],
});

describe("QueryEditor - aggregate reducer", () => {
  describe("updateQuery action", () => {
    const query: RecordQuery = {
      offset: 0,
      limit: 10,
      rows: ["baz"],
    };
    const state = queryEditorRecordReducer(initialRecordState, {
      type: "[record] updateQuery",
      payload: query,
    });

    it("should set a new query", () => {
      expect(state.query).toEqual(query);
    });
    it("should set the previous query", () => {
      expect(state.previousQueries).toEqual([initialRecordState.query]);
    });
    it("should still have visual mode", () => {
      expect(state.mode).toEqual("visual");
    });
    it("should have nothing in nextQueries", () => {
      expect(state.nextQueries).toEqual([]);
    });
    it("should not create new history entry if the query is the same", () => {
      const newState = queryEditorRecordReducer(state, { type: "[record] updateQuery", payload: query });
      expect(newState).toEqual({
        query,
        previousQueries: [initialRecordState.query],
        nextQueries: [],
        mode: "visual",
      });
    });
  });

  describe("clear action", () => {
    const query: RecordQuery = {
      offset: 0,
      limit: 10,
      rows: ["baz"],
    };
    const state = queryEditorRecordReducer(
      { ...initialRecordState, query },
      {
        type: "[record] clearQuery",
      },
    );

    it("should reset the query", () => {
      expect(state.query).toEqual(initialRecordState.query);
    });
    it("should set the previous query", () => {
      expect(state.previousQueries).toEqual([query]);
    });
    it("should still have visual mode", () => {
      expect(state.mode).toEqual("visual");
    });
    it("should have nothing in nextQueries", () => {
      expect(state.nextQueries).toEqual([]);
    });
  });

  describe("undo", () => {
    it("should do nothing if I don't have any history", () => {
      const state = queryEditorRecordReducer(
        {
          mode: "code",
          query: createRecordFakeQuery("current"),
          previousQueries: [],
          nextQueries: [createRecordFakeQuery("next 1")],
        },
        { type: "[record] undo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createRecordFakeQuery("current"),
        previousQueries: [],
        nextQueries: [createRecordFakeQuery("next 1")],
      });
    });

    it("should move the history", () => {
      const state = queryEditorRecordReducer(
        {
          mode: "code",
          query: createRecordFakeQuery("current"),
          previousQueries: [createRecordFakeQuery("previous 2"), createRecordFakeQuery("previous 1")],
          nextQueries: [],
        },
        { type: "[record] undo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createRecordFakeQuery("previous 1"),
        previousQueries: [createRecordFakeQuery("previous 2")],
        nextQueries: [createRecordFakeQuery("current")],
      });
    });
  });

  describe("redo", () => {
    it("should do nothing if I don't have any history", () => {
      const state = queryEditorRecordReducer(
        {
          mode: "code",
          query: createRecordFakeQuery("current"),
          previousQueries: [createRecordFakeQuery("previous 1")],
          nextQueries: [],
        },
        { type: "[record] redo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createRecordFakeQuery("current"),
        previousQueries: [createRecordFakeQuery("previous 1")],
        nextQueries: [],
      });
    });

    it("should move the history", () => {
      const state = queryEditorRecordReducer(
        {
          mode: "code",
          query: createRecordFakeQuery("current"),
          previousQueries: [createRecordFakeQuery("previous 1"), createRecordFakeQuery("previous 2")],
          nextQueries: [createRecordFakeQuery("next 1"), createRecordFakeQuery("next 2")],
        },
        { type: "[record] redo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createRecordFakeQuery("next 1"),
        previousQueries: [
          createRecordFakeQuery("current"),
          createRecordFakeQuery("previous 1"),
          createRecordFakeQuery("previous 2"),
        ],
        nextQueries: [createRecordFakeQuery("next 2")],
      });
    });
  });

  describe("switchMode", () => {
    it("should switch to code", () => {
      const state = queryEditorRecordReducer(
        {
          mode: "code",
          query: createRecordFakeQuery("current"),
          previousQueries: [createRecordFakeQuery("previous 1")],
          nextQueries: [],
        },
        { type: "[record] switchMode", payload: "visual" },
      );

      expect(state).toEqual({
        mode: "visual",
        query: createRecordFakeQuery("current"),
        previousQueries: [createRecordFakeQuery("previous 1")],
        nextQueries: [],
      });
    });
  });
});
