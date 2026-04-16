import init, { LogootSplitSystem } from "./pkg/logoot_plus.js"

type DiffResult = { pos: number, del: number, ins: string }

// ---------- Diff ----------
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

// ---------- Helpers ----------
const elemById = (name: string): HTMLElement => {
  const elem = document.getElementById(name)
  if (elem == null) throw Error('Missing element ' + name)
  return elem
}

// ---------- Editor ----------
const attachEditor = (agentId: number, elemName: string, system: any) => {
  const elem = elemById(elemName) as HTMLTextAreaElement

  let lastValue = system.read(agentId)
  let applying = false

  elem.value = lastValue

  elem.addEventListener('input', () => {
    if (applying) return

    const newValue = elem.value.replace(/\r\n/g, '\n')
    if (newValue === lastValue) return

    const { pos, del, ins } = calcDiff(lastValue, newValue)

    if (del > 0) system.del(agentId, pos, pos + del)
    if (ins !== '') system.ins(agentId, pos, ins)

    // Refresh from CRDT
    lastValue = system.read(agentId)
    elem.value = lastValue

    console.log(system.getDebugBlocks(agentId))
  })

  return {
    syncFrom(fromId: number) {
      applying = true
      system.syncFrom(agentId, fromId)
      lastValue = system.read(agentId)
      elem.value = lastValue
      applying = false
    },

    reset() {
      // Optional: implement in Rust if needed
      applying = true
      lastValue = system.read(agentId)
      elem.value = lastValue
      applying = false
    }
  }
}

// ---------- Main ----------
window.onload = async () => {
  try {
    await init("./pkg/logoot_plus_bg.wasm")
    console.log('WASM Backend Ready!')

    const system = new LogootSplitSystem(3)

    const a = attachEditor(0, 'text1', system)
    const b = attachEditor(1, 'text2', system)
    const c = attachEditor(2, 'text3', system)

    const wire = (btnId: string, target: any, fromId: number) => {
      elemById(btnId).onclick = () => target.syncFrom(fromId)
    }

    // Pairwise sync
    wire('2to1', a, 1)
    wire('3to1', a, 2)

    wire('1to2', b, 0)
    wire('3to2', b, 2)

    wire('1to3', c, 0)
    wire('2to3', c, 1)

    elemById('reset').onclick = () => {
      console.log('Resetting...')
      system.reset()
      a.reset()
      b.reset()
      c.reset()
    }

  } catch (err) {
    console.error("Initialization failed:", err)
  }
}