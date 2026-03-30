import SeedRandom from "seed-random";
import assert from 'node:assert/strict'
import { Document } from "./logoot.js";

function fuzzer(seed: number) {
  const random = SeedRandom(`${seed}`)

  const randInt = (n: number) => Math.floor(random() * n)
  const randBool = (weight: number = 0.5) => random() < weight

  const randPosInt = (n: number) => {
    let x = Math.floor(random() * n)
    if (x === 0) {
      return randPosInt(n)
    }
  }

  const alphabet = [...' abcdefghijklmnopqrstuvwxyz']
  const randChar = () => alphabet[randInt(alphabet.length)]

  const docs = [
    new Document(0),
    new Document(1),
    new Document(2),
  ]

  const randDoc = () => docs[randInt(3)]

  for (let i = 0; i < 100; i++) {
    // console.log('ii', i)
    for (let d = 0; d < 3; d++) {
      // 1. Pick a random document
      // 2. Make a random change to that document
      const doc = randDoc()!
      const len = doc.snapshot.length

      const insertWeight = len < 100 ? 0.65 : 0.35

      if (len === 0 || randBool(insertWeight)) {
        // Insert
        const strlen = randPosInt(10)
        const content = [...Array(strlen)].map(randChar).join('')
        const pos = randInt(len + 1)
        doc.ins(pos, content)
      } else {
        // delete
        const from = randInt(len)
        const to = from + randInt(Math.min(len - from, 10))
        doc.del(from, to)
      }
    }

    // pick 2 documents and merge them
    const a = randDoc()!
    const b = randDoc()!

    if (a === b) continue

    a.mergeFrom(b)
    b.mergeFrom(a)
    assert.deepEqual(a.snapshot, b.snapshot)
    // assert.deepEqual(a.branch.snapshot, b.branch.snapshot)
  }
}

for (let i = 0; i < 1000; i++) {
  console.log('seed', i)
  fuzzer(i)
}