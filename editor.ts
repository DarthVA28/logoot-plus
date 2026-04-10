// This file implements a super simple text editor using textarea on top of the
// CRDT implementation.
// Code adapted from Seph Gentle's tutorial on eg-walker 
// https://github.com/josephg/egwalker-from-scratch

import init, { Document } from "./pkg/logoot_plus.js"

type DiffResult = { pos: number, del: number, ins: string }

// Simple diff function
const calcDiff = (oldval: string, newval: string): DiffResult => {
  if (oldval === newval) return { pos: 0, del: 0, ins: '' }

  let oldChars = [...oldval]
  let newChars = [...newval]

  let commonStart = 0
  while (oldChars[commonStart] === newChars[commonStart]) {
    commonStart++
  }

  let commonEnd = 0
  while (
    oldChars[oldChars.length - 1 - commonEnd] === newChars[newChars.length - 1 - commonEnd] &&
    commonEnd + commonStart < oldChars.length &&
    commonEnd + commonStart < newChars.length
  ) {
    commonEnd++
  }

  const del =
    oldChars.length !== commonStart + commonEnd
      ? oldChars.length - commonStart - commonEnd
      : 0

  const ins =
    newChars.length !== commonStart + commonEnd
      ? newChars.slice(commonStart, newChars.length - commonEnd).join('')
      : ''

  return { pos: commonStart, del, ins }
}

const elemById = (name: string): HTMLElement => {
  const elem = document.getElementById(name)
  if (elem == null) throw Error('Missing element ' + name)
  return elem
}

const attachEditor = (agentName: number, elemName: string) => {
  const elem = elemById(elemName) as HTMLTextAreaElement

  const doc = new Document(agentName)
  let lastValue = doc.read()
  let applying = false

  elem.value = lastValue

  // Only listen to actual text changes
  elem.addEventListener('input', () => {
    if (applying) return

    const newValue = elem.value.replace(/\r\n/g, '\n')
    if (newValue === lastValue) return

    const { pos, del, ins } = calcDiff(lastValue, newValue)

    if (del > 0) doc.del(pos, pos + del)

    if (ins !== '') doc.ins(pos, ins)

    // Always sync from CRDT
    lastValue = doc.read()
    elem.value = lastValue

    console.log(doc.getDebugBlocks())
  })

  return {
    doc,

    reset() {
      applying = true
      doc.reset()
      lastValue = doc.read()
      elem.value = lastValue
      applying = false
    },

    mergeFrom(other: Document) {
      applying = true
      doc.mergeFrom(other)
      lastValue = doc.read()
      elem.value = lastValue
      applying = false
    }
  }
}

// ... (keep calcDiff, elemById, and attachEditor exactly as they are) ...

window.onload = async () => {
  try {
    await init("./pkg/logoot_plus_bg.wasm");
    console.log('WASM Backend Ready!');

    const a = attachEditor(0, 'text1');
    const b = attachEditor(1, 'text2');
    const c = attachEditor(2, 'text3');

    // Helper to wire up buttons quickly
    const wire = (btnId: string, target: any, source: any) => {
      elemById(btnId).onclick = () => target.mergeFrom(source.doc);
    };

    // Pairwise Merges
    wire('2to1', a, b);
    wire('3to1', a, c);
    
    wire('1to2', b, a);
    wire('3to2', b, c);
    
    wire('1to3', c, a);
    wire('2to3', c, b);

    elemById('reset').onclick = () => {
      a.reset(); b.reset(); c.reset();
    };

  } catch (err) {
    console.error("Initialization failed:", err);
  }
};