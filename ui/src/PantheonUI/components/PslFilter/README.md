# Psl Filter

This component is responsible to help the user on Psl Filter creation. We are using Antlr4 to parse the filter string to an AST to provide a nice visual way to see the filter.

## Main files

`index.tsx`: The component ready to use.
`PslFilter.g4`: Grammar file for Antlr, stolen from pantheon backend.
`parser/*`: Antlr generated files (see next section)

## How to regenerated the `parser` folder

If something change in the `PslFilter.g4`, you need to re-generate new lexer/tokenizer with `antlr4-tool`. You need to have antlr installed on your computer (so java), if fabien is around, just ask! You are alone or you like challenge? -> https://www.antlr.org

When you have antlr installed, you can just run `yarn gen:psl-filter-parser`.

Have fun!

## Visual Specs

https://app.zeplin.io/project/5b22288192fd37d946cb6acb/screen/5c98d067a7cf9c0609c75f68
