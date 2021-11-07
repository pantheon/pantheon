import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import { Reducer } from "react";

import pslFilter, {
  FilterTree,
  FilterTreeOperatorNode,
  FilterTreePredicateNode,
  fixFilterValue,
  isOperator,
  isPredicate,
  unmatchedParenthesis,
} from "../PslFilter/parseFilter";

import { OrderedColumn } from "../queries";
import { QueryEditorActions } from "./QueryEditor.reducer";

export interface AggregateQuery {
  rows: string[];
  columns: string[];
  measures: string[];
  offset: number;
  limit: number;
  orderBy?: OrderedColumn[];
  filter?: string;
  postFilter?: string;
}

export interface QueryEditorAggregateState {
  mode: "visual" | "code";
  query: AggregateQuery;
  previousQueries: AggregateQuery[];
  nextQueries: AggregateQuery[];
  sections: {
    measures: boolean;
    rows: boolean;
    topNRows: boolean;
    columns: boolean;
    topNColumns: boolean;
    filters: boolean;
    postAggregateFilters: boolean;
    order: boolean;
    limitAndOffset: boolean;
  };
}

export const initialAggregateState: QueryEditorAggregateState = {
  mode: "visual",
  query: {
    columns: [],
    measures: [],
    rows: [],
    offset: 0,
    limit: 10,
  },
  previousQueries: [],
  nextQueries: [],
  // `true` = collapsed
  sections: {
    measures: false,
    rows: false,
    topNRows: true,
    columns: false,
    topNColumns: false,
    filters: false,
    postAggregateFilters: true,
    order: false,
    limitAndOffset: false,
  },
};

export const isQueryEditorAggregateActions = (action: QueryEditorActions): action is QueryEditorAggregateActions =>
  action.type.startsWith("[aggregate]");

export type QueryEditorAggregateActions =
  | { type: "[aggregate] updateQuery"; payload: AggregateQuery }
  | { type: "[aggregate] clearQuery" }
  | { type: "[aggregate] addMeasure"; payload: { ref: string; index?: number } }
  | { type: "[aggregate] removeMeasure"; payload: { ref: string } }
  | { type: "[aggregate] reorderMeasure"; payload: { ref: string; startIndex: number; endIndex: number } }
  | { type: "[aggregate] addRow"; payload: { ref: string; index?: number } }
  | { type: "[aggregate] removeRow"; payload: { ref: string } }
  | { type: "[aggregate] reorderRow"; payload: { ref: string; startIndex: number; endIndex: number } }
  | { type: "[aggregate] addColumn"; payload: { ref: string; index?: number } }
  | { type: "[aggregate] removeColumn"; payload: { ref: string } }
  | { type: "[aggregate] reorderColumn"; payload: { ref: string; startIndex: number; endIndex: number } }
  | { type: "[aggregate] addOrderBy"; payload: { ref: string; index: number } }
  | { type: "[aggregate] updateOrderBy"; payload: { ref: string; order: OrderedColumn["order"] } }
  | { type: "[aggregate] reorderOrderBy"; payload: { ref: string; startIndex: number; endIndex: number } }
  | { type: "[aggregate] removeOrderBy"; payload: { ref: string } }
  | { type: "[aggregate] addFilterPredicate"; payload: FilterTreePredicateNode & { index: number } }
  | { type: "[aggregate] updateFilterPredicate"; payload: FilterTreePredicateNode & { index: number } }
  | { type: "[aggregate] removeFilterPredicate"; payload: { index: number } }
  | { type: "[aggregate] reorderFilterPredicate"; payload: { startIndex: number; endIndex: number } }
  | {
      type: "[aggregate] addParenthesesInFilter";
      payload: { openParenthesisIndex: number; closeParenthesisIndex: number };
    }
  | {
      type: "[aggregate] removeParenthesesInFilter";
      payload: { parenthesisId: number };
    }
  | { type: "[aggregate] toggleFilterOperator"; payload: { index: number } }
  | { type: "[aggregate] switchRowColumn"; payload: { ref: string } }
  | { type: "[aggregate] undo" }
  | { type: "[aggregate] redo" }
  | { type: "[aggregate] switchMode"; payload: QueryEditorAggregateState["mode"] }
  | { type: "[aggregate] toggleSection"; payload: keyof QueryEditorAggregateState["sections"] };

// a little function to help us with reordering the result
const reorder = <T>(list: T[], startIndex: number, endIndex: number): T[] => {
  const result = Array.from(list);
  const [removed] = result.splice(startIndex, 1);
  result.splice(endIndex, 0, removed);

  return result;
};

export const queryEditorAggregateReducer: Reducer<QueryEditorAggregateState, QueryEditorAggregateActions> = (
  prevState,
  action,
) => {
  switch (action.type) {
    case "[aggregate] clearQuery":
      return {
        ...prevState,
        query: initialAggregateState.query,
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[aggregate] updateQuery":
      if (isEqual(prevState.query, action.payload)) {
        return prevState; // prevent duplicate queries in the history
      }
      return {
        ...prevState,
        query: action.payload,
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };

    // Measure
    case "[aggregate] addMeasure": {
      const { index, ref } = action.payload;
      if (prevState.query.measures.includes(ref)) {
        return prevState;
      }
      const measures =
        index === undefined
          ? [...prevState.query.measures, ref]
          : [...prevState.query.measures.slice(0, index), ref, ...prevState.query.measures.slice(index)];

      return {
        ...prevState,
        query: { ...prevState.query, measures },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] removeMeasure":
      return {
        ...prevState,
        query: { ...prevState.query, measures: prevState.query.measures.filter(m => m !== action.payload.ref) },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[aggregate] reorderMeasure": {
      const { startIndex, endIndex } = action.payload;
      const measures = reorder(prevState.query.measures, startIndex, endIndex);
      return {
        ...prevState,
        query: { ...prevState.query, measures },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }

    // Row
    case "[aggregate] addRow": {
      const { index, ref } = action.payload;
      if (prevState.query.rows.includes(ref)) {
        return prevState;
      }
      const rows =
        index === undefined
          ? [...prevState.query.rows, ref]
          : [...prevState.query.rows.slice(0, index), ref, ...prevState.query.rows.slice(index)];

      return {
        ...prevState,
        query: { ...prevState.query, rows, columns: prevState.query.columns.filter(c => c !== ref) },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] removeRow":
      return {
        ...prevState,
        query: {
          ...prevState.query,
          rows: prevState.query.rows.filter(r => r !== action.payload.ref),
          orderBy: (prevState.query.orderBy || []).filter(i => i.name !== action.payload.ref),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[aggregate] reorderRow": {
      const { startIndex, endIndex } = action.payload;
      const rows = reorder(prevState.query.rows, startIndex, endIndex);
      return {
        ...prevState,
        query: { ...prevState.query, rows },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }

    // Column
    case "[aggregate] addColumn": {
      const { index, ref } = action.payload;
      if (prevState.query.columns.includes(ref)) {
        return prevState;
      }
      const columns =
        index === undefined
          ? [...prevState.query.columns, ref]
          : [...prevState.query.columns.slice(0, index), ref, ...prevState.query.columns.slice(index)];

      return {
        ...prevState,
        query: { ...prevState.query, columns, rows: prevState.query.rows.filter(r => r !== ref) },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] removeColumn":
      return {
        ...prevState,
        query: { ...prevState.query, columns: prevState.query.columns.filter(c => c !== action.payload.ref) },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[aggregate] reorderColumn": {
      const { startIndex, endIndex } = action.payload;
      const columns = reorder(prevState.query.columns, startIndex, endIndex);
      return {
        ...prevState,
        query: { ...prevState.query, columns },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }

    case "[aggregate] switchRowColumn": {
      const { ref } = action.payload;
      const isRefInColumns = prevState.query.columns.includes(ref);
      const isRefInRows = prevState.query.rows.includes(ref);
      if (!isRefInColumns && !isRefInRows) {
        return prevState;
      } else if (isRefInColumns) {
        return {
          ...prevState,
          query: {
            ...prevState.query,
            columns: prevState.query.columns.filter(c => c !== ref),
            rows: [...prevState.query.rows, ref],
          },
          previousQueries: [...prevState.previousQueries, prevState.query],
          nextQueries: [],
        };
      } else {
        return {
          ...prevState,
          query: {
            ...prevState.query,
            rows: prevState.query.rows.filter(r => r !== ref),
            orderBy: (prevState.query.orderBy || []).filter(i => i.name !== ref),
            columns: [...prevState.query.columns, ref],
          },
          previousQueries: [...prevState.previousQueries, prevState.query],
          nextQueries: [],
        };
      }
    }

    case "[aggregate] addOrderBy": {
      const { orderBy = [] } = prevState.query;
      const { index, ref } = action.payload;
      const isMeasure = ref.split(".").length === 1;

      const query: AggregateQuery = isMeasure
        ? {
            ...prevState.query,
            measures: prevState.query.measures.includes(ref)
              ? prevState.query.measures
              : [...prevState.query.measures, ref],
            orderBy: [...orderBy.slice(0, index), { name: ref, order: "Desc" }, ...orderBy.slice(index)],
          }
        : {
            ...prevState.query,
            columns: prevState.query.columns.filter(c => c !== ref),
            rows: prevState.query.rows.includes(ref) ? prevState.query.rows : [...prevState.query.rows, ref],
            orderBy: [...orderBy.slice(0, index), { name: ref, order: "Desc" }, ...orderBy.slice(index)],
          };

      return {
        ...prevState,
        query,
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }

    case "[aggregate] updateOrderBy":
      return {
        ...prevState,
        query: {
          ...prevState.query,
          orderBy: (prevState.query.orderBy || []).map(i =>
            i.name === action.payload.ref ? { ...i, order: action.payload.order } : i,
          ),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };

    case "[aggregate] reorderOrderBy":
      return {
        ...prevState,
        query: {
          ...prevState.query,
          orderBy: reorder(prevState.query.orderBy || [], action.payload.startIndex, action.payload.endIndex),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };

    case "[aggregate] removeOrderBy":
      return {
        ...prevState,
        query: {
          ...prevState.query,
          orderBy: (prevState.query.orderBy || []).filter(i => i.name !== action.payload.ref),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };

    case "[aggregate] undo":
      if (isEmpty(prevState.previousQueries)) {
        return prevState;
      } else {
        return {
          ...prevState,
          query: prevState.previousQueries[prevState.previousQueries.length - 1],
          previousQueries: prevState.previousQueries.slice(0, -1),
          nextQueries: [prevState.query, ...prevState.nextQueries],
        };
      }
    case "[aggregate] redo":
      if (isEmpty(prevState.nextQueries)) {
        return prevState;
      } else {
        return {
          ...prevState,
          query: prevState.nextQueries[0],
          previousQueries: [...prevState.previousQueries, prevState.query],
          nextQueries: prevState.nextQueries.slice(1),
        };
      }
    case "[aggregate] switchMode":
      return {
        ...prevState,
        mode: action.payload,
      };
    case "[aggregate] toggleSection":
      return {
        ...prevState,
        sections: { ...prevState.sections, [action.payload]: !prevState.sections[action.payload] },
      };
    case "[aggregate] reorderFilterPredicate": {
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      const prevPredicate = prevTree.filter(isPredicate);
      const nextPredicate = reorder(prevPredicate, action.payload.startIndex, action.payload.endIndex);
      let j = 0;
      const nextTree = prevTree.map(node => (isPredicate(node) ? nextPredicate[j++] : node));

      const filter = pslFilter.stringify(nextTree);
      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter,
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] addParenthesesInFilter": {
      const { openParenthesisIndex, closeParenthesisIndex } = action.payload;

      if (openParenthesisIndex >= closeParenthesisIndex) {
        throw new Error("A close parenthese must be after the open one!");
      }
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      if (closeParenthesisIndex > prevTree.length) {
        throw new Error("Close parenthese index must be in tree range");
      }
      if (prevTree[openParenthesisIndex].type === "predicate" || prevTree[closeParenthesisIndex].type === "predicate") {
        throw new Error("You can't add a parenthese to a predicate node");
      }

      const filter = pslFilter.stringify(
        prevTree.map((node, i) =>
          node.type === "predicate"
            ? node
            : {
                ...node,
                openParentheses:
                  i === openParenthesisIndex ? [unmatchedParenthesis, ...node.openParentheses] : node.openParentheses,
                closeParentheses:
                  i === closeParenthesisIndex
                    ? [...node.closeParentheses, unmatchedParenthesis]
                    : node.closeParentheses,
              },
        ),
      );

      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter,
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] toggleFilterOperator": {
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      if (!prevTree[action.payload.index] || !prevTree[action.payload.index].operator) {
        throw new Error(`There is no operator with the index ${action.payload.index}`);
      }

      const filter = pslFilter.stringify(
        prevTree.map((node, i) =>
          i === action.payload.index
            ? ({ ...node, operator: node.operator === "and" ? "or" : "and" } as FilterTreeOperatorNode)
            : node,
        ),
      );

      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter,
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] removeParenthesesInFilter": {
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      const filter = pslFilter.stringify(
        prevTree.map(node =>
          node.type === "predicate"
            ? node
            : {
                ...node,
                openParentheses: node.openParentheses.filter(id => id !== action.payload.parenthesisId),
                closeParentheses: node.closeParentheses.filter(id => id !== action.payload.parenthesisId),
              },
        ),
      );
      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter,
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] addFilterPredicate": {
      const { dimension, operator, index } = action.payload;
      const value = fixFilterValue(action.payload.value);

      const prevTree = pslFilter.parse(prevState.query.filter || "");
      const newTree: FilterTree =
        prevTree.length === 0
          ? [{ type: "predicate", dimension, operator, value }]
          : [
              ...prevTree.slice(0, index * 2),
              ...(index === 0
                ? ([
                    { type: "predicate", dimension, operator, value },
                    { type: "operator", operator: "and", openParentheses: [], closeParentheses: [] },
                  ] as FilterTree)
                : ([
                    { type: "operator", operator: "and", openParentheses: [], closeParentheses: [] },
                    { type: "predicate", dimension, operator, value },
                  ] as FilterTree)),
              ...prevTree.slice(index * 2),
            ];

      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter: pslFilter.stringify(newTree),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] updateFilterPredicate": {
      const { dimension, operator, index } = action.payload;
      const value = fixFilterValue(action.payload.value);
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      const newTree = prevTree.map((node, i) =>
        i === index * 2 + 1 ? ({ type: "predicate", dimension, operator, value } as FilterTreePredicateNode) : node,
      );
      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter: pslFilter.stringify(newTree),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    case "[aggregate] removeFilterPredicate": {
      // we can drag and drop only predicates, but our state stores predicates and operators in the same list
      // [operator, predicate, operator, predicate]
      // this means that we need to convert index to position in the list using following formula
      // index*2 - operator above, index*2 + 1 - the predicate, index*2 +2 - operator below

      const { index } = action.payload;
      const prevTree = pslFilter.parse(prevState.query.filter || "");
      const associatedOperator = prevTree[index * 2] as FilterTreeOperatorNode;
      const parenthesesToRemove = [...associatedOperator.closeParentheses, ...associatedOperator.openParentheses];
      // remove predicate and operator below it
      const newTree = [...prevTree.slice(0, index * 2), ...prevTree.slice(index * 2 + 2)].map((node, i) =>
        isOperator(node)
          ? {
              ...node,
              operator: i === 0 ? undefined : node.operator,
              openParentheses: node.openParentheses.filter(id => !parenthesesToRemove.includes(id)),
              closeParentheses: node.closeParentheses.filter(id => !parenthesesToRemove.includes(id)),
            }
          : node,
      );
      return {
        ...prevState,
        query: {
          ...prevState.query,
          filter: pslFilter.stringify(newTree),
        },
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    }
    default:
      return prevState;
  }
};
