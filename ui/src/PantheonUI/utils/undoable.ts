/**
 * A simple, TypeScript-first undo-redo data type (functor) wrapping any value.
 * Implements https://redux.js.org/recipes/implementingundohistory#designing-the-algorithm.
 */
export interface Undoable<T> {
  _past: T[];
  _present: T;
  _future: T[];
}

export const of = <T>(val: T) => ({
  _past: [],
  _present: val,
  _future: [],
});

export const clearHistory = <T>(undoable: Undoable<T>): Undoable<T> => {
  return {
    _past: [],
    _present: undoable._present,
    _future: [],
  };
};

export const getPresent = <T>(undoable: Undoable<T>): T => {
  return undoable._present;
};

export const setPresent = <T>(newPresent: T, undoable: Undoable<T>): Undoable<T> => {
  return {
    _past: trimFront([...undoable._past, undoable._present], 50),
    _present: newPresent,
    _future: [],
  };
};

export const canUndo = <T>(undoable: Undoable<T>): boolean => {
  return undoable._past.length > 0;
};

export const canRedo = <T>(undoable: Undoable<T>): boolean => {
  return undoable._future.length > 0;
};

export const undo = <T>(undoable: Undoable<T>): Undoable<T> => {
  if (!canUndo(undoable)) {
    return undoable;
  }
  return {
    _past: undoable._past.slice(0, undoable._past.length - 1),
    _present: undoable._past[undoable._past.length - 1],
    _future: trimBack([undoable._present, ...undoable._future], 50),
  };
};

export const redo = <T>(undoable: Undoable<T>): Undoable<T> => {
  if (!canRedo(undoable)) {
    return undoable;
  }
  return {
    _past: trimFront([...undoable._past, undoable._present], 50),
    _present: undoable._future[0],
    _future: undoable._future.slice(1),
  };
};

// Trim the front of an array if it is longer than the maximum allowed
const trimFront = <T>(list: T[], maxMembers: number): T[] => {
  if (list.length > maxMembers) {
    return list.slice(-maxMembers);
  }
  return list;
};

// Trim the back of an array if it is longer than the maximum allowed
const trimBack = <T>(list: T[], maxMembers: number): T[] => {
  if (list.length > maxMembers) {
    return list.slice(0, maxMembers);
  }
  return list;
};
