import {
  AggregateQuery,
  initialAggregateState,
  queryEditorAggregateReducer,
  QueryEditorAggregateState,
} from "./aggregate.reducer";

export const createAggregateFakeQuery = (id: string): AggregateQuery => ({
  columns: [id],
  measures: [id],
  offset: 0,
  limit: 10,
  rows: [id],
});

describe("QueryEditor - aggregate reducer", () => {
  describe("updateQuery action", () => {
    const query: AggregateQuery = {
      columns: ["foo"],
      measures: ["bar"],
      offset: 0,
      limit: 10,
      rows: ["baz"],
    };
    const state = queryEditorAggregateReducer(initialAggregateState, {
      type: "[aggregate] updateQuery",
      payload: query,
    });

    it("should set a new query", () => {
      expect(state.query).toEqual(query);
    });
    it("should set the previous query", () => {
      expect(state.previousQueries).toEqual([initialAggregateState.query]);
    });
    it("should still have visual mode", () => {
      expect(state.mode).toEqual("visual");
    });
    it("should have nothing in nextQueries", () => {
      expect(state.nextQueries).toEqual([]);
    });
    it("should not create new history entry if the query is the same", () => {
      const newState = queryEditorAggregateReducer(state, { type: "[aggregate] updateQuery", payload: query });
      expect(newState).toEqual({
        query,
        sections: initialAggregateState.sections,
        previousQueries: [initialAggregateState.query],
        nextQueries: [],
        mode: "visual",
      });
    });
  });

  describe("add measure", () => {
    it("should do nothing if the measure is already set", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, measures: ["ref"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addMeasure",
        payload: { ref: "ref" },
      });

      expect(newState).toEqual(state);
    });
    it("should add a measure on an empty array", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, measures: [] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addMeasure",
        payload: { ref: "newMeasure" },
      });

      expect(newState.query.measures).toEqual(["newMeasure"]);
    });
    it("should add a measure to the end if no order", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, measures: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addMeasure",
        payload: { ref: "newMeasure" },
      });

      expect(newState.query.measures).toEqual(["one", "two", "newMeasure"]);
    });
    it("should add a measure on index 1", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, measures: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addMeasure",
        payload: { ref: "newMeasure", index: 1 },
      });

      expect(newState.query.measures).toEqual(["one", "newMeasure", "two"]);
    });
    it("should add a measure on index 0", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, measures: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addMeasure",
        payload: { ref: "newMeasure", index: 0 },
      });

      expect(newState.query.measures).toEqual(["newMeasure", "one", "two"]);
    });
  });

  describe("remove measure", () => {
    it("should remove measure", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, measures: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeMeasure",
        payload: { ref: "two" },
      });

      expect(newState.query.measures).toEqual(["one", "three"]);
    });
  });

  describe("reorder measure", () => {
    it("should reorder a measure", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, measures: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] reorderMeasure",
        payload: { ref: "two", startIndex: 1, endIndex: 0 },
      });

      expect(newState.query.measures).toEqual(["two", "one", "three"]);
    });
  });

  describe("add row", () => {
    it("should do nothing if the row is already set", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, rows: ["ref"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "ref" },
      });

      expect(newState).toEqual(state);
    });
    it("should add a row on an empty array", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, rows: [] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "newRow" },
      });

      expect(newState.query.rows).toEqual(["newRow"]);
    });
    it("should add a row to the end if no order", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, rows: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "newRow" },
      });

      expect(newState.query.rows).toEqual(["one", "two", "newRow"]);
    });
    it("should add a row on index 1", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, rows: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "newRow", index: 1 },
      });

      expect(newState.query.rows).toEqual(["one", "newRow", "two"]);
    });
    it("should add a row on index 0", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, rows: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "newRow", index: 0 },
      });

      expect(newState.query.rows).toEqual(["newRow", "one", "two"]);
    });
    it("should add an entry in the history", () => {
      const query = { ...initialAggregateState.query, rows: ["one", "two"] };
      const state = { ...initialAggregateState, query };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "newRow", index: 0 },
      });

      expect(newState.previousQueries).toEqual([query]);
    });
    it("should remove dimension from columns", () => {
      const query = { ...initialAggregateState.query, rows: ["one", "two"], columns: ["three"] };
      const state = { ...initialAggregateState, query };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addRow",
        payload: { ref: "three", index: 0 },
      });

      expect(newState.query.columns).toEqual([]);
    });
  });

  describe("remove row", () => {
    it("should remove row", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, rows: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeRow",
        payload: { ref: "two" },
      });

      expect(newState.query.rows).toEqual(["one", "three"]);
    });

    it("should remove related orderBy row", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          rows: ["one", "two", "three"],
          orderBy: [{ name: "one", order: "Desc" }, { name: "tree", order: "Asc" }],
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeRow",
        payload: { ref: "one" },
      });

      expect(newState.query.orderBy).toEqual([{ name: "tree", order: "Asc" }]);
    });
  });

  describe("reorder row", () => {
    it("should reorder a row", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, rows: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] reorderRow",
        payload: { ref: "two", startIndex: 1, endIndex: 0 },
      });

      expect(newState.query.rows).toEqual(["two", "one", "three"]);
    });
  });

  describe("add column", () => {
    it("should do nothing if the column is already set", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, columns: ["ref"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "ref" },
      });

      expect(newState).toEqual(state);
    });
    it("should add a column on an empty array", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, columns: [] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "newColumn" },
      });

      expect(newState.query.columns).toEqual(["newColumn"]);
    });
    it("should add a column to the end if no order", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, columns: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "newColumn" },
      });

      expect(newState.query.columns).toEqual(["one", "two", "newColumn"]);
    });
    it("should add a column on index 1", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, columns: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "newColumn", index: 1 },
      });

      expect(newState.query.columns).toEqual(["one", "newColumn", "two"]);
    });
    it("should add a column on index 0", () => {
      const state = { ...initialAggregateState, query: { ...initialAggregateState.query, columns: ["one", "two"] } };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "newColumn", index: 0 },
      });

      expect(newState.query.columns).toEqual(["newColumn", "one", "two"]);
    });
    it("should remove dimension from rows", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two"], rows: ["three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addColumn",
        payload: { ref: "three", index: 0 },
      });

      expect(newState.query.rows).toEqual([]);
    });
  });

  describe("remove column", () => {
    it("should remove column", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeColumn",
        payload: { ref: "two" },
      });

      expect(newState.query.columns).toEqual(["one", "three"]);
    });
  });

  describe("reorder column", () => {
    it("should reorder a column", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two", "three"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] reorderColumn",
        payload: { ref: "two", startIndex: 1, endIndex: 0 },
      });

      expect(newState.query.columns).toEqual(["two", "one", "three"]);
    });
  });

  describe("switchRowColumn", () => {
    it("should move the dimension from columns to rows", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two", "three"], rows: ["four"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] switchRowColumn",
        payload: { ref: "two" },
      });
      expect(newState.nextQueries.length).toBe(0);
      expect(newState.previousQueries.length).toBe(1);
      expect(newState.query.columns).toEqual(["one", "three"]);
      expect(newState.query.rows).toEqual(["four", "two"]);
    });

    it("should move the dimension from rows to columns", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two", "three"], rows: ["four"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] switchRowColumn",
        payload: { ref: "four" },
      });
      expect(newState.nextQueries.length).toBe(0);
      expect(newState.previousQueries.length).toBe(1);
      expect(newState.query.columns).toEqual(["one", "two", "three", "four"]);
      expect(newState.query.rows).toEqual([]);
    });

    it("should do nothing if the dimension is not in rows/columns", () => {
      const state = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, columns: ["one", "two", "three"], rows: ["four"] },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] switchRowColumn",
        payload: { ref: "wtf" },
      });
      expect(newState.nextQueries.length).toBe(0);
      expect(newState.previousQueries.length).toBe(0);
      expect(newState.query.columns).toEqual(["one", "two", "three"]);
      expect(newState.query.rows).toEqual(["four"]);
    });
    it("should remove related orderBy row", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          columns: ["one", "two", "three"],
          rows: ["four"],
          orderBy: [{ name: "four", order: "Desc" }],
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] switchRowColumn",
        payload: { ref: "four" },
      });

      expect(newState.query.orderBy).toEqual([]);
    });
  });

  describe("clear action", () => {
    const query: AggregateQuery = {
      columns: ["foo"],
      measures: ["bar"],
      offset: 0,
      limit: 10,
      rows: ["baz"],
    };
    const state = queryEditorAggregateReducer(
      { ...initialAggregateState, query },
      {
        type: "[aggregate] clearQuery",
      },
    );

    it("should reset the query", () => {
      expect(state.query).toEqual(initialAggregateState.query);
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
      const state = queryEditorAggregateReducer(
        {
          mode: "code",
          query: createAggregateFakeQuery("current"),
          previousQueries: [],
          nextQueries: [createAggregateFakeQuery("next 1")],
          sections: initialAggregateState.sections,
        },
        { type: "[aggregate] undo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createAggregateFakeQuery("current"),
        previousQueries: [],
        nextQueries: [createAggregateFakeQuery("next 1")],
        sections: initialAggregateState.sections,
      });
    });

    it("should move the history", () => {
      const state0: QueryEditorAggregateState = {
        ...initialAggregateState,
        nextQueries: [],
        previousQueries: [],
        query: createAggregateFakeQuery("1"),
      };
      const state1 = queryEditorAggregateReducer(state0, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("2"),
      });
      const state2 = queryEditorAggregateReducer(state1, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("3"),
      });
      const state3 = queryEditorAggregateReducer(state2, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("4"),
      });
      const state4 = queryEditorAggregateReducer(state3, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("5"),
      });
      const state5 = queryEditorAggregateReducer(state4, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("6"),
      });
      const state6 = queryEditorAggregateReducer(state5, {
        type: "[aggregate] updateQuery",
        payload: createAggregateFakeQuery("7"),
      });
      const state7 = queryEditorAggregateReducer(state6, { type: "[aggregate] undo" });
      const state8 = queryEditorAggregateReducer(state7, { type: "[aggregate] undo" });
      const state9 = queryEditorAggregateReducer(state8, { type: "[aggregate] undo" });

      expect(state6.previousQueries).toEqual(["1", "2", "3", "4", "5", "6"].map(createAggregateFakeQuery));
      expect(state6.query).toEqual(createAggregateFakeQuery("7"));
      expect(state6.nextQueries).toEqual([]);

      expect(state7.previousQueries).toEqual(["1", "2", "3", "4", "5"].map(createAggregateFakeQuery));
      expect(state7.query).toEqual(createAggregateFakeQuery("6"));
      expect(state7.nextQueries).toEqual([createAggregateFakeQuery("7")]);

      expect(state8.previousQueries).toEqual(["1", "2", "3", "4"].map(createAggregateFakeQuery));
      expect(state8.query).toEqual(createAggregateFakeQuery("5"));
      expect(state8.nextQueries).toEqual(["6", "7"].map(createAggregateFakeQuery));

      expect(state9.previousQueries).toEqual(["1", "2", "3"].map(createAggregateFakeQuery));
      expect(state9.query).toEqual(createAggregateFakeQuery("4"));
      expect(state9.nextQueries).toEqual(["5", "6", "7"].map(createAggregateFakeQuery));
    });
  });

  describe("redo", () => {
    it("should do nothing if I don't have any history", () => {
      const state = queryEditorAggregateReducer(
        {
          mode: "code",
          query: createAggregateFakeQuery("current"),
          previousQueries: [createAggregateFakeQuery("previous 1")],
          nextQueries: [],
          sections: initialAggregateState.sections,
        },
        { type: "[aggregate] redo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createAggregateFakeQuery("current"),
        previousQueries: [createAggregateFakeQuery("previous 1")],
        nextQueries: [],
        sections: initialAggregateState.sections,
      });
    });

    it("should move the history", () => {
      const state = queryEditorAggregateReducer(
        {
          mode: "code",
          query: createAggregateFakeQuery("4"),
          previousQueries: ["1", "2", "3"].map(createAggregateFakeQuery),
          nextQueries: ["5", "6", "7"].map(createAggregateFakeQuery),
          sections: initialAggregateState.sections,
        },
        { type: "[aggregate] redo" },
      );

      expect(state).toEqual({
        mode: "code",
        query: createAggregateFakeQuery("5"),
        previousQueries: ["1", "2", "3", "4"].map(createAggregateFakeQuery),
        nextQueries: ["6", "7"].map(createAggregateFakeQuery),
        sections: initialAggregateState.sections,
      });
    });
  });

  describe("switchMode", () => {
    it("should switch to code", () => {
      const state = queryEditorAggregateReducer(
        {
          mode: "code",
          query: createAggregateFakeQuery("current"),
          previousQueries: [createAggregateFakeQuery("previous 1")],
          nextQueries: [],
          sections: initialAggregateState.sections,
        },
        { type: "[aggregate] switchMode", payload: "visual" },
      );

      expect(state).toEqual({
        mode: "visual",
        query: createAggregateFakeQuery("current"),
        previousQueries: [createAggregateFakeQuery("previous 1")],
        nextQueries: [],
        sections: initialAggregateState.sections,
      });
    });
  });

  describe("toggle section", () => {
    it("should toggle a section", () => {
      const state = queryEditorAggregateReducer(initialAggregateState, {
        type: "[aggregate] toggleSection",
        payload: "measures",
      });

      expect(state.sections).toEqual({ ...initialAggregateState.sections, measures: true });
    });
  });

  describe("reorder orderBy", () => {
    it("should reorder orderBy list", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          orderBy: [{ name: "foo", order: "Asc" }, { name: "bar", order: "Desc" }],
        },
      };

      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] reorderOrderBy",
        payload: { ref: "bar", startIndex: 0, endIndex: 1 },
      });

      expect(newState.query.orderBy).toEqual([{ name: "bar", order: "Desc" }, { name: "foo", order: "Asc" }]);
    });
  });

  describe("update orderBy", () => {
    it("should update orderBy", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          orderBy: [{ name: "foo", order: "Asc" }, { name: "bar", order: "Desc" }],
        },
      };

      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] updateOrderBy",
        payload: { ref: "foo", order: "Desc" },
      });

      expect(newState.query.orderBy).toEqual([{ name: "foo", order: "Desc" }, { name: "bar", order: "Desc" }]);
    });
  });

  describe("add orderBy", () => {
    it("should add the first orderBy", () => {
      const newState = queryEditorAggregateReducer(initialAggregateState, {
        type: "[aggregate] addOrderBy",
        payload: { ref: "foo", index: 0 },
      });

      expect(newState.query.orderBy).toEqual([{ name: "foo", order: "Desc" }]);
    });

    it("should add an item to orderBy", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          orderBy: [{ name: "store.foo", order: "Asc" }, { name: "store.bar", order: "Desc" }],
        },
      };

      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addOrderBy",
        payload: { ref: "store.baz", index: 1 },
      });

      expect(newState.query.orderBy).toEqual([
        { name: "store.foo", order: "Asc" },
        { name: "store.baz", order: "Desc" },
        { name: "store.bar", order: "Desc" },
      ]);
    });

    it("should add a row if this one not exists", () => {
      const newState = queryEditorAggregateReducer(initialAggregateState, {
        type: "[aggregate] addOrderBy",
        payload: { ref: "store.foo", index: 0 },
      });

      expect(newState.query.rows).toEqual(["store.foo"]);
    });

    it("should remove the dimension from columns", () => {
      const newState = queryEditorAggregateReducer(
        { ...initialAggregateState, query: { ...initialAggregateState.query, columns: ["store.foo"] } },
        {
          type: "[aggregate] addOrderBy",
          payload: { ref: "store.foo", index: 0 },
        },
      );

      expect(newState.query.columns).toEqual([]);
    });
    it("should add a measure if this one not exists", () => {
      const newState = queryEditorAggregateReducer(
        { ...initialAggregateState, query: { ...initialAggregateState.query } },
        {
          type: "[aggregate] addOrderBy",
          payload: { ref: "foo", index: 0 },
        },
      );

      expect(newState.query.measures).toEqual(["foo"]);
    });
  });

  describe("remove orderBy", () => {
    it("should remove an item from orderBy", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          orderBy: [{ name: "foo", order: "Asc" }, { name: "bar", order: "Desc" }],
        },
      };

      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeOrderBy",
        payload: { ref: "bar" },
      });

      expect(newState.query.orderBy).toEqual([{ name: "foo", order: "Asc" }]);
    });
  });

  describe("reorder filter predicate", () => {
    it("should reorder the predicate inside the filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "((customer.city = 'Paris' or customer.city = 'Berlin') and product.brand = 'Fabulous')",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] reorderFilterPredicate",
        payload: {
          startIndex: 0,
          endIndex: 2,
        },
      });

      expect(newState.query.filter).toEqual(
        "((customer.city = 'Berlin' or product.brand = 'Fabulous') and customer.city = 'Paris')",
      );
    });
  });

  describe("add parenthesis in filter", () => {
    it("should add some parentheses in the current filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "customer.city = 'Paris' and customer.city = 'Berlin' and product.brand = 'Fabulous'",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addParenthesesInFilter",
        payload: {
          openParenthesisIndex: 0,
          closeParenthesisIndex: 4,
        },
      });

      expect(newState.query.filter).toEqual(
        "(customer.city = 'Paris' and customer.city = 'Berlin') and product.brand = 'Fabulous'",
      );
    });
  });

  describe("remove parentheses in filter", () => {
    it("should remove both matching parentheses", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "(customer.city = 'Paris' and customer.city = 'Berlin') and product.brand = 'Fabulous'",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeParenthesesInFilter",
        payload: {
          parenthesisId: 0,
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' and customer.city = 'Berlin' and product.brand = 'Fabulous'",
      );
    });

    it("should remove both matching parentheses on nested case", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "((customer.city = 'Paris' or customer.city = 'Berlin') and product.brand = 'Fabulous')",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeParenthesesInFilter",
        payload: {
          parenthesisId: 1,
        },
      });

      expect(newState.query.filter).toEqual(
        "(customer.city = 'Paris' or customer.city = 'Berlin') and product.brand = 'Fabulous'",
      );
    });
  });

  describe("toggle filter operator", () => {
    it("should replace `and` with `or` on index 2", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "customer.city = 'Paris' and customer.city = 'Berlin' and product.brand = 'Fabulous'",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] toggleFilterOperator",
        payload: {
          index: 2,
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' or customer.city = 'Berlin' and product.brand = 'Fabulous'",
      );
    });
    it("should replace `or` with `and` on index 4", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "customer.city = 'Paris' and customer.city = 'Berlin' and product.brand = 'Fabulous'",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] toggleFilterOperator",
        payload: {
          index: 4,
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' and customer.city = 'Berlin' or product.brand = 'Fabulous'",
      );
    });
  });

  describe("add filter predicate", () => {
    it("should add a predicate into an empty filter", () => {
      const newState = queryEditorAggregateReducer(initialAggregateState, {
        type: "[aggregate] addFilterPredicate",
        payload: {
          dimension: "customer.city",
          index: 0,
          operator: "=",
          value: "'Paris'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Paris'");
    });

    it("should add the last predicate of filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addFilterPredicate",
        payload: {
          dimension: "product.brand",
          index: 2,
          operator: "=",
          value: "'Fabulous'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' and customer.city = 'Berlin' and product.brand = 'Fabulous'",
      );
    });

    it("should add the first predicate of filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addFilterPredicate",
        payload: {
          dimension: "product.brand",
          index: 0,
          operator: "=",
          value: "'Fabulous'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual(
        "product.brand = 'Fabulous' and customer.city = 'Paris' and customer.city = 'Berlin'",
      );
    });

    it("should add the predicate in middle of a filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addFilterPredicate",
        payload: {
          dimension: "product.brand",
          index: 1,
          operator: "=",
          value: "'Fabulous'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' and product.brand = 'Fabulous' and customer.city = 'Berlin'",
      );
    });

    it("should add a list in the filter", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] addFilterPredicate",
        payload: {
          dimension: "product.brand",
          index: 1,
          operator: "in",
          value: "'Fabulous', 'Test'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual(
        "customer.city = 'Paris' and product.brand in ('Fabulous', 'Test') and customer.city = 'Berlin'",
      );
    });
  });

  describe("update filter predicate", () => {
    it("should update the given predicate", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] updateFilterPredicate",
        payload: {
          dimension: "product.brand",
          index: 1,
          operator: "=",
          value: "'Fabulous'",
          type: "predicate",
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Paris' and product.brand = 'Fabulous'");
    });
  });

  describe("remove filter predicate", () => {
    it("should remove the first predicate", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeFilterPredicate",
        payload: {
          index: 0,
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Berlin'");
    });

    it("should remove the last predicate", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: { ...initialAggregateState.query, filter: "customer.city = 'Paris' and customer.city = 'Berlin'" },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeFilterPredicate",
        payload: {
          index: 1,
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Paris'");
    });

    it("should remove the last predicate and keep parentheses", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "customer.city = 'Paris' and (customer.city = 'Berlin' or customer.city = 'Paris')",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeFilterPredicate",
        payload: {
          index: 2,
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Paris' and (customer.city = 'Berlin')");
    });

    it("should remove the middle predicate and associate parentheses", () => {
      const state: QueryEditorAggregateState = {
        ...initialAggregateState,
        query: {
          ...initialAggregateState.query,
          filter: "customer.city = 'Paris' and (customer.city = 'Berlin' or customer.city = 'Paris')",
        },
      };
      const newState = queryEditorAggregateReducer(state, {
        type: "[aggregate] removeFilterPredicate",
        payload: {
          index: 1,
        },
      });

      expect(newState.query.filter).toEqual("customer.city = 'Paris' or customer.city = 'Paris'");
    });
  });
});
