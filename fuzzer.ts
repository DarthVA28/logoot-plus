import SeedRandom from "seed-random";
import assert from 'node:assert/strict'
import { Document } from "./index.js";

function fuzzer(seed: number) {
  const random = SeedRandom(`${seed}`)

  const randInt = (n: number) => {
    return Math.floor(random() * n)
  }

  const randPosInt = (n: number) => {
    let x = Math.floor(random() * n)
    if (x === 0) {
      return randPosInt(n)
    }
  }

  const randBool = (weight: number = 0.5) => random() < weight

  const alphabet = [...'abcdefghijklmnopqrstuvwxyz']
  const randChar = () => alphabet[randInt(alphabet.length)]

  const docs = [
    new Document(0),
    new Document(1),
    new Document(2),
  ]

  // if (seed == 240) {
  //   for (const doc of docs) {
  //     doc.DEBUG = true
  //   }
  // }

  const randDoc = () => docs[randInt(2)]

  for (let i = 0; i < 100; i++) {
    // console.log('ii', i)
    for (let d = 0; d < 3; d++) {
      // 1. Pick a random document
      // 2. Make a random change to that document
      const doc = randDoc()!
      doc.read() // update snapshot
      const len = doc.snapshot.length
      const strlen = randPosInt(3)
      const content = [...Array(strlen)].map(randChar).join('')
      const pos = randInt(len + 1)
      if (doc.DEBUG) {
        console.log("Inserting", content, "at", pos, "in doc", doc.state.replica)
      }
      doc.ins(pos, content)
    //   doc.read()
      // doc after inserting: 
    //   console.table(doc.blocks)
    }

    // pick 2 documents and merge them
    const a = randDoc()!
    let b = randDoc()!
    // while (true) {
    //   if (a !== b) {
    //     break
    //   }
    //   b = randDoc()!
    // }
    // const b = randDoc()!

    if (a === b) continue
    
    if (a.DEBUG && b.DEBUG) {
      console.log("Before merging, tables are: ")
      console.log("Table for", a.state.replica)
      console.table(a.blocks)
      console.log('---')
      console.log("Table for", b.state.replica)
      console.table(b.blocks)
    }

    a.mergeFrom(b)
    b.mergeFrom(a)

    a.read()
    b.read()

    // console.log("After merging:")


    // console.log('a:', a.read())
    // console.log('b:', b.read())
    assert.deepEqual(a.snapshot, b.snapshot)
    // assert.deepEqual(a.branch.snapshot, b.branch.snapshot)
  }

  // console.log(docs[0]?.snapshot)
}

// fuzzer(4)
// console.log('fuzzer done')

for (let i = 0; i < 1000; i++) {
  console.log('starting seed:', i)
  fuzzer(i)
  console.log('seed done:', i)
}