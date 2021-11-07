export const sliceTextIfNeeded = (text: string = "", maxLength: number = 30) =>
  (text || "").length > maxLength ? text.slice(0, maxLength) + "…" : text;
