# logoot-plus

This is an implementation of a collaborative text editor using Collaborative Replicated Datatypes (CRDTs). We use the LogootSplit algorithm, which is an optimized version of Logoot which supports *adaptive granularity* of the CRDT elements. 

## Setting up the project

```bash
bun install
```

To run:

```bash
bun run index.ts
```

This project was created using `bun init` in bun v1.3.11. [Bun](https://bun.com) is a fast all-in-one JavaScript runtime.
