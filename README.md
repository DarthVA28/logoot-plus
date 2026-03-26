# logoot-plus

This is an implementation of a collaborative text editor using Collaborative Replicated Datatypes (CRDTs). We use the LogootSplit algorithm, which is an optimized version of Logoot which supports *adaptive granularity* of the CRDT elements. 

## Setting up the project

```bash
bun install
```

## Running the Editor

```bash
bun build editor.ts --outfile=bundle.js --target=browser
```

Then open `index.html` in the browser of your choice

## Fuzz Testing

For testing using fuzzer:

```bash
bun run fuzzer.ts
```