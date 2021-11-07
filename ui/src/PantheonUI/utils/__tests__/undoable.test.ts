import curry from "lodash/curry";
import * as undoable from "../undoable";

/**
 * Applies a list of functions to the same value (re-implementing here for stricter typing)
 * Note that functions are applied the other way, e.g.
 * `simpleCompose([x => x + 1, x => 2 * x])(2)` first multiplies.
 */
const simpleCompose = <T>(funcs: Array<((val: T) => T)>): ((val: T) => T) => {
  const [head, ...tail] = funcs;
  if (!head) {
    return (val: T) => val;
  }
  return (val: T) => head(simpleCompose(tail)(val));
};

test("creates an expected structure", () => {
  expect(undoable.of(2)).toEqual({
    _past: [],
    _present: 2,
    _future: [],
  });
});

test("set + set + undo", () => {
  const ops = simpleCompose([undoable.undo, curry(undoable.setPresent)(3)]);
  expect(ops(undoable.of(2))).toEqual({
    _past: [],
    _present: 2,
    _future: [3],
  });
});

test("Stops after many undos", () => {
  const initial = undoable.of(2);
  const ops = simpleCompose([undoable.undo, undoable.undo, curry(undoable.setPresent)(3)]);
  expect(ops(initial)).toEqual({
    _past: [],
    _present: 2,
    _future: [3],
  });
});

test("Stops after many redos", () => {
  const initial = undoable.of(2);
  const ops = simpleCompose([
    undoable.redo,
    undoable.redo,
    undoable.redo,
    undoable.undo,
    undoable.undo,
    curry(undoable.setPresent)(3),
  ]);
  expect(ops(initial)).toEqual({
    _past: [2],
    _present: 3,
    _future: [],
  });
});
