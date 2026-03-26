# logoot-plus

This is an implementation of a collaborative text editor using Collaborative Replicated Datatypes (CRDTs). We use the LogootSplit algorithm, which is an optimized version of Logoot which supports *adaptive granularity* of the CRDT elements. 

**LogootSplit Paper:** Supporting Adaptable Granularity of Changes for Massive-scale Collaborative Editing, 9th IEEE International Conference on Collaborative Computing: Networking, Applications and Worksharing, Luc Andre, Stephane Martin, Gerald Oster, Claudia-Lavinia Ignat

https://members.loria.fr/CIgnat/files/pdf/AndreCollabCom13.pdf

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
