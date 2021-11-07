import { FilterTree, fixFilterValue, parseFilter, printFilter } from "./parseFilter";

describe("PslFilter", () => {
  const tests: Array<{ name: string; filter: string; tree: FilterTree; only?: boolean }> = [
    {
      name: "a simple filter",
      filter: "customer.city = 'Paris'",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a simple filter (dimension multival operator)",
      filter: "customer.city in ('Paris', 'Berlin')",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "in", value: "'Paris', 'Berlin'" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a simple filter (dimension unary operator)",
      filter: "customer.city is not null",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "is not null" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a filter with a `or` operator",
      filter: "customer.city = 'Paris' or customer.city = 'Berlin'",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", operator: "or", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a filter with a `and` operator",
      filter: "customer.city = 'Paris' and customer.city = 'Berlin'",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", operator: "and", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a filter with a parentheses",
      filter: "(customer.city = 'Paris' or customer.city = 'Berlin') and product.brand = 'Fabulous'",
      tree: [
        { type: "operator", openParentheses: [0], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", operator: "or", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", openParentheses: [], closeParentheses: [0], operator: "and" },
        { type: "predicate", dimension: "product.brand", operator: "=", value: "'Fabulous'" },
        { type: "operator", openParentheses: [], closeParentheses: [] },
      ],
    },
    {
      name: "a filter with a nested parentheses",
      filter: "((customer.city = 'Paris' or customer.city = 'Berlin') and product.brand = 'Fabulous')",
      tree: [
        { type: "operator", openParentheses: [1, 0], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", operator: "or", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", openParentheses: [], closeParentheses: [0], operator: "and" },
        { type: "predicate", dimension: "product.brand", operator: "=", value: "'Fabulous'" },
        { type: "operator", openParentheses: [], closeParentheses: [1] },
      ],
    },
    {
      name: "a filter with parentheses on the second group",
      filter: "customer.city = 'Paris' and (customer.city = 'Berlin' and product.brand = 'Fabulous')",
      tree: [
        { type: "operator", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", openParentheses: [0], closeParentheses: [], operator: "and" },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", operator: "and", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "product.brand", operator: "=", value: "'Fabulous'" },
        { type: "operator", openParentheses: [], closeParentheses: [0] },
      ],
    },
    {
      name: "a filter with a multiple parentheses",
      filter: "(customer.city = 'Paris' or customer.city = 'Berlin') and (product.brand = 'Fabulous')",
      tree: [
        { type: "operator", openParentheses: [0], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Paris'" },
        { type: "operator", operator: "or", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.city", operator: "=", value: "'Berlin'" },
        { type: "operator", closeParentheses: [0], openParentheses: [1], operator: "and" },
        { type: "predicate", dimension: "product.brand", operator: "=", value: "'Fabulous'" },
        { type: "operator", openParentheses: [], closeParentheses: [1] },
      ],
    },
    {
      name: "a filter with non string values",
      filter: "(customer.count = 2 or customer.birthdate = date '2019-02-01') and (product.awesome = true)",
      tree: [
        { type: "operator", openParentheses: [0], closeParentheses: [] },
        { type: "predicate", dimension: "customer.count", operator: "=", value: "2" },
        { type: "operator", operator: "or", openParentheses: [], closeParentheses: [] },
        { type: "predicate", dimension: "customer.birthdate", operator: "=", value: "date '2019-02-01'" },
        { type: "operator", closeParentheses: [0], openParentheses: [1], operator: "and" },
        { type: "predicate", dimension: "product.awesome", operator: "=", value: "true" },
        { type: "operator", openParentheses: [], closeParentheses: [1] },
      ],
    },
    {
      name: "an empty filter",
      filter: "",
      tree: [],
    },
  ];

  describe("parseFilter", () => {
    tests.forEach(test =>
      (test.only ? it.only : it)(`should parse ${test.name}`, () =>
        expect(parseFilter(test.filter)).toEqual(test.tree),
      ),
    );
  });

  describe("printFilter", () => {
    tests.forEach(test =>
      (test.only ? it.only : it)(`should print ${test.name}`, () =>
        expect(printFilter(test.tree)).toEqual(test.filter),
      ),
    );
  });

  describe("fixFilterValue", () => {
    const inputExpected: Array<[string, string]> = [
      ["date '2019-02-01 12:00:00'", "date '2019-02-01 12:00:00'"],
      ["timestamp '2019-02-01'", "timestamp '2019-02-01'"],
      ["true", "true"],
      ["false", "false"],
      ["true, true", "true, true"],
      ["2", "2"],
      ["2, 3, 6", "2, 3, 6"],
      ["'hello'", "'hello'"],
      ["'hello', 'world'", "'hello', 'world'"],
      ["hello", "'hello'"],
      ['"hello"', "'hello'"],
      ["'hello", "'hello'"],
      ["hello'", "'hello'"],
      ["hello, world", "'hello', 'world'"],
      [undefined, undefined],
    ];
    inputExpected.forEach(([input, expected]) =>
      it(`should fix ${input}`, () => {
        expect(fixFilterValue(input)).toEqual(expected);
      }),
    );
  });
});
