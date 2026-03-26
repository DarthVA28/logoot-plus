/* 
An implementation of the LOGOOT algorithm for collaborative text editing   
*/

import assert from "assert"
import SeedRandom from "seed-random";

// Identifier for each block 
type Id = number[]

type Range = [number, number]

const random = SeedRandom('42')

const MIN_VALUE = 0
const MAX_VALUE = 50
const SEPARATOR = -1

type State = {
    localClock: number
    replica: number // id of the replica 
}

class InfiniteIterator<T> {
    elements: T[]
    infinity: T
    current: number  

    constructor(elements: T[], infinity: T) {
        this.elements = elements
        this.infinity = infinity
        this.current = 0
    }

    next() : T {
        if (this.current < this.elements.length) {
            let x = this.elements[this.current++]!
            return x
        } else {
            return this.infinity
        }
    }
}

/* Generates a new base between idLow and idHigh */
function generateBase(idLow : Id, idHigh : Id, state: State) : Id {
    let low = new InfiniteIterator(idLow, MIN_VALUE)
    let high = new InfiniteIterator(idHigh, MAX_VALUE)

    const newBase : Id = []

    let l = low.next()
    let h = high.next()

    while (h-l < 2) {
        newBase.push(l)
        l = low.next()
        h = high.next()
    }

    // random character between MIN_VALUE and MAX_VALUE
    const nxt = l + 1 + Math.floor(random() * (h - l - 1))
    newBase.push(nxt)
    newBase.push(state.replica)
    newBase.push(state.localClock)

    return newBase
}

type Operation = {
    type: 'add' | 'del'
    ids: Id[] 
    offsets: number[] 
    payload: string | null
    site: number
    clock: number
}

type Block = { 
    base: Id
    offsets: Range
    value: string
    size: number
    creator: number // the replica which created this block
}

export class Document {
    blocks: Block[]
    state: State
    idUsedRanges: Map<Id, Range> // The range of offsets for an ID which are used 
    snapshot: string 
    operations: Operation[]
    lastSeen: Map<number, number>
    DEBUG: boolean = false

    constructor(replica: number) {
        this.blocks = []
        this.state = {
            localClock: 0,
            replica: replica
        }
        this.idUsedRanges = new Map()
        this.snapshot = ""
        this.operations = []
        this.lastSeen = new Map()
    }

    // Add insert function 
    ins(pos: number, text: string) : Operation {
        const op = localInsert(this, pos, text)
        this.operations.push(op)
        this.state.localClock++
        if (this.DEBUG) {
            console.log("After local insert at site ", this.state.replica, " with operation: ")
            console.table(this.blocks)
        }
        return op
    }

    del(from: number, to: number) : Operation {
        const op = localDelete(this, from, to)
        this.operations.push(op)
        this.state.localClock++
        if (this.DEBUG) {
            console.log("After local delete at site ", this.state.replica, " with operation: ")
            console.table(this.blocks)
        }
        return op
    }

    read() : string {
        let res = ""
        for (let block of this.blocks) {
            res += block.value
        }
        this.snapshot = res
        return res
    }

    // Merge changes from another document
    mergeFrom(other: Document) : void {
        for (let op of other.operations) {
            // if we have already seen this operation, ignore it 
            if (this.lastSeen.get(op.site) !== undefined && this.lastSeen.get(op.site)! >= op.clock) {
                continue
            }
            this.lastSeen.set(op.site, op.clock)
            if (op.type === 'del') {
                remoteDelete(this, op)
            } else {
                remoteInsert(this, op)
            }
            if (this.DEBUG) {
                console.log("After merging operation from site ", op.site.toString(), " with id ", op.ids[0])
                console.table(this.blocks)
            }
            if (this.DEBUG) {
                // Validate the document! Check each entry in blocks, see if ID ordering is correct, if there are any entries with the same ID and offsets etc
                for (let i = 0; i < this.blocks.length - 1; i++) {
                    const block1 = this.blocks[i]!
                    const block2 = this.blocks[i+1]!
                    const maxId1 = block1.base.concat([block1.offsets[1]-1])
                    const minId2 = block2.base.concat([block2.offsets[0]])
                    if (compareIds(maxId1, minId2) >= 0) {
                        console.log("ID ordering is incorrect between blocks at index ", i, " and ", i+1)
                        console.log("Block at index ", i, ": ", block1)
                        console.log("Block at index ", i+1, ": ", block2)
                        throw new Error("ID ordering is incorrect")
                    }
                }
                // Check for same ID + offsets
                const idOffsetMap = new Map<string, number>()
                for (let block of this.blocks) {
                    const id = block.base.toString()
                    const offsets = block.offsets.toString()
                    const key = id + "-" + offsets
                    if (idOffsetMap.has(key)) {
                        console.log("Duplicate ID+offsets found: ", key)
                        throw new Error("Duplicate ID+offsets found")
                    }
                    idOffsetMap.set(key, 1)
                }
            }
        }
    }
}

type PosInfo = { 
    idx : number, 
    offset: number
}

/* Count number of items you can insert between two IDs */
function numInsertable(idInsert: Id, idNext: Id, length: number) : number {
    const l = idInsert.length - 1
    if (l >= idNext.length) return length 
    else {
        for (let i = 0; i < l; i++) {
            if (idInsert[i] !== idNext[i]) {
                return length
            }
        }
        // idInsert is a prefix of idNext
        return idNext[l]! - idInsert[l]! + 1
    }
}

function findInsertIndex(doc: Document, pos: number) : PosInfo | null {
    if (doc.blocks.length == 0) return null
    let offset = 0
    for (let i = 0 ; i < doc.blocks.length ; i++) {
        const block = doc.blocks[i]!
        if (offset + block.size >= pos) {
            // pos is in or at the end of this block
            return {
                idx: i,
                offset: offset
            }
        }
        offset += block.size
    }
    // pos is at the end of the document
    return {
        idx: doc.blocks.length - 1,
        offset: offset // check
    }
}

function findDeleteIndex(doc: Document, pos: number) : PosInfo | null {
    if (doc.blocks.length == 0) return null
    let offset = 0
    for (let i = 0 ; i < doc.blocks.length ; i++) {
        const block = doc.blocks[i]!
        if (offset + block.size > pos) {
            // pos is in this block
            return {
                idx: i,
                offset: offset
            }
        }
        offset += block.size
    }
    // pos is at the end of the document, return null since there is nothing to delete
    return null
}

/* Returns 1 if id1 > id2, 0 if equal, -1 otherwise */
function compareIds(id1: Id, id2: Id) : number { 
    const l = Math.min(id1.length, id2.length)
    for (let i = 0; i < l; i++) {
        if (id1[i]! < id2[i]!) return -1
        else if (id1[i]! > id2[i]!) return 1
    }
    if (id1.length > id2.length) return 1
    else if (id1.length < id2.length) return -1
    else return 0
    // return 0
}

function searchBlock(doc: Document, id: Id) : number | null {
    // Find the block with the largest id which is smaller than id
    if (doc.blocks.length == 0) return null 

    // const first = doc.blocks[0]!
    // const minId = first.base.concat([first.offsets[0]])
    // if (compareIds(minId, id) >= 0) return null

    let idx = 0
    while (idx < doc.blocks.length) {
        const block = doc.blocks[idx]!
        const maxId = block.base.concat([block.offsets[1]-1])
        if (compareIds(maxId, id) >= 0) {
            break
        }
        idx++
    }
    if (idx === doc.blocks.length) return idx - 1
    const block = doc.blocks[idx]!
    const minId = block.base.concat([block.offsets[0]])
    if (compareIds(id, minId) >= 0) return idx  // id is inside this block
    return idx == 0 ? null : idx - 1
}

function searchBlockContainingId(doc: Document, id: Id) : PosInfo | null {
    if (doc.blocks.length == 0) throw new Error("Document is empty!")
    for (let i = 0; i < doc.blocks.length; i++) {
        const block = doc.blocks[i]!
        const minId = block.base.concat([block.offsets[0]])
        const maxId = block.base.concat([block.offsets[1]-1])
        if (compareIds(id, minId) >= 0 && compareIds(id, maxId) <= 0) {
            // Find the exact offset of the id in the block
            let offset = -1
            for (let j = 0; j < block.size; j++) {
                const idElem = block.base.concat([block.offsets[0] + j])
                if (compareIds(idElem, id) == 0) {
                    offset = j
                    break
                }
            }
            if (offset == -1) throw new Error("ID not found in the block")
            return {
                idx: i,
                offset: offset
            }
        }
    }
    return null
}

function insertNewBlockAtIdx(doc: Document, text: string, idx: number | null, site: number, newId: Id = []) : Operation { 
    if (doc.DEBUG) {
        console.log("newId passed: ", newId)
    }
    let lOffset = 0
    let rOffset = text.length
    /* We want to insert a new block between idx and idx + 1 */
    if (newId.length == 0) {
        let idLow : Id = []
        let idHigh : Id = []
        
        if (idx != null) {
            if (idx > -1) {
                const block = doc.blocks[idx]!
                idLow = block.base.concat([block.offsets[1]-1])
            } 
            if (idx + 1 < doc.blocks.length) {  
                const next = doc.blocks[idx + 1]!
                idHigh = next.base.concat([next.offsets[0]])
            } else {
                // check if it works
                idHigh = [MAX_VALUE]
            }
        }
        
        newId = generateBase(idLow, idHigh, doc.state)
        if (doc.DEBUG) {
            console.log("Generated id ", newId, "between ", idLow, " and ", idHigh)
        }
    } else {
        // check if the ID exists in the map 
        if (doc.idUsedRanges.has(newId)) {
            // lookup the max used by it
            const usedRange = doc.idUsedRanges.get(newId)!
            if (doc.DEBUG) {
                console.log("ID ", newId, " already exists with used range ", usedRange)
            }
            if (doc.DEBUG) {
                // print the entire usedRanges map
                console.log("Current idUsedRanges map: ")
                for (let [key, value] of doc.idUsedRanges.entries()) {
                    console.log("ID: ", key, " Used Range: ", value)
                }
            }
            lOffset = usedRange[1]
            rOffset = usedRange[1] + text.length
        }
    }

    doc.idUsedRanges.set(newId, [0, rOffset])
    const newBlock : Block = {
        base: newId,
        offsets: [lOffset, rOffset],
        value: text,
        size: text.length,
        creator: site
    }    
    doc.blocks.splice(idx == null ? 0 : idx + 1, 0, newBlock)
    // doc.idUsedRanges.set(newId, [0, text.length])
    return {
        type: 'add',
        ids: [newId],
        offsets: [0],
        payload: text,
        site: site,
        clock: doc.state.localClock
    }
}

function extendBlock(doc: Document, idx: number, text: string, site: number) : Operation { 
    const block = doc.blocks[idx]!
    const usedRange = doc.idUsedRanges.get(block.base)!

    // Check if we can extend the block without clashing with the identifiers 
    if (idx + 1 < doc.blocks.length) { 
        const next = doc.blocks[idx + 1]!
        let idInsert = block.base.concat([block.offsets[1]])
        let idNext = next.base.concat([next.offsets[0]])
        let n = numInsertable(idInsert, idNext, text.length)
        if (n < text.length) {
            // Cannot extend the block without clashing with the next block's identifiers
            // Insert n characters here and then insert a new block 
            block.value += text.substring(0, n)
            block.size += n
            block.offsets[1] += n
            const newRanges : Range = [usedRange[0], block.offsets[1]]
            doc.idUsedRanges.set(block.base, newRanges)
            return insertNewBlockAtIdx(doc, text.substring(n), idx, site)
        }
    }

    block.value += text
    block.size += text.length
    block.offsets[1] += text.length
    const newRanges : Range = [usedRange[0], block.offsets[1]]
    doc.idUsedRanges.set(block.base, newRanges)
    return { 
        type: 'add',
        ids: [block.base],
        offsets: [usedRange[1]],
        payload: text,
        site: site,
        clock: doc.state.localClock
    }
}

function splitAndInsertBlock(doc: Document, idx: number, sp: number, text: string, site: number, newId: Id = []) : Operation {
    const block = doc.blocks[idx]!

    // Split the current block into two 
    const block1 : Block = {
        base: block.base,
        offsets: [block.offsets[0], block.offsets[0] + sp],
        value: block.value.substring(0, sp),
        size: sp,
        creator: block.creator
    }

    const block2 : Block = {
        base: block.base,
        offsets: [block.offsets[0] + sp, block.offsets[1]],
        value: block.value.substring(sp),
        size: block.size - sp,
        creator: block.creator
    }

    if (newId.length == 0) {
        const idLow = block.base.concat([block.offsets[0] + sp - 1]) 
        const idHigh = block.base.concat([block.offsets[0] + sp]) 
        newId = generateBase(idLow, idHigh, doc.state)
    }

    const newBlock : Block = { 
        base: newId, 
        offsets: [0, text.length],
        value: text,
        size: text.length,
        creator: site // doc.state.replica   
    }

    doc.blocks.splice(idx, 1, block1, newBlock, block2)
    doc.idUsedRanges.set(newId, [0, text.length])
    return { 
        type: 'add',
        ids: [newId],
        offsets: [0], 
        payload: text,
        site: site,
        clock: doc.state.localClock
    }

}

function localInsert(doc: Document, pos: number, text: string) : Operation { 
    // Invariant: Size of text passed to the localInsert is less than MAXVALUE 
    assert(text.length < MAX_VALUE, "Text length passed to insert must be less than " + MAX_VALUE)

    let posInfo = findInsertIndex(doc, pos)
    if (posInfo === null) {
        // document is empty
        if (doc.DEBUG) {
            console.log("Document is empty, inserting at the start")
        }
        return insertNewBlockAtIdx(doc, text, null, doc.state.replica)
    }

    const { idx, offset } = posInfo
    const block = doc.blocks[idx]!

    // If we are inserting at the end of the block
    // And we are the creator of the block 
    // And the block endpoint is maximal 
    if (pos == offset + block.size) { 
        if (block.creator == doc.state.replica) {
            const usedRange = doc.idUsedRanges.get(block.base)
            if (usedRange === undefined) throw new Error(`Block ${block.base} has no used range`)
            if (usedRange[1] == block.offsets[1]) {
                return extendBlock(doc, idx, text, doc.state.replica)
            }
        }

        // Cannot be extended: Insert between this and next block (if it exists)
        if (doc.DEBUG) {
            console.log("Inserting a block at the end of block index: ", idx)
        }
        return insertNewBlockAtIdx(doc, text, idx, doc.state.replica)
    }

    // // If we are inserting at the start of the block 
    // // Create a new block and insert it before the current block
    if (pos == offset) {
        if (doc.DEBUG) {
            console.log("Inserting a block at the start of block index: ", idx)
        }
        return insertNewBlockAtIdx(doc, text, idx - 1, doc.state.replica)
    }

    // Split the block and insert in between
    const sp = pos - offset
    if (doc.DEBUG) {
        console.log("Inserting in the middle of block index ", idx)
        console.log("Split point in the block is ", sp)
    }
    return splitAndInsertBlock(doc, idx, sp, text, doc.state.replica)
}

function findSplitPoint(block: Block, id: Id) : number { 
    // Find the point in the block where the new ID should be inserted
    let sp = 0
    for (let i = 0; i < block.value.length; i++) {
        const idElem = block.base.concat([block.offsets[0] + i])
        if (compareIds(idElem, id) >= 0) {
            break
        }
        sp++
    }
    return sp 
}


function remoteInsert(doc: Document, op: Operation) : void { 
    // console.log("Received remote operation with id ", op.ids[0])
    // Assuming op is an add operation, throw an error otherwise 
    if (op.type !== 'add') throw new Error("Expected an add operation")

    if (doc.DEBUG) {
        console.log("At site ", doc.state.replica, "Merging operation from site ", op.site.toString(), " with id ", op.ids[0])
        console.table(op)
    }
    
    const base = op.ids[0]!
    const offset = op.offsets[0]!
    const text = op.payload!
    const site = op.site

    // Find the block with largerst identifier smaller than id 
    const id = base.concat([offset])
    const idx = searchBlock(doc, id)
    // console.log("Searching for ID ", id, "Found block index: ", idx)
    if (idx === null) {
        insertNewBlockAtIdx(doc, text, null, site, base)
        return
    }

    const block = doc.blocks[idx]!

    // check if the IDs match and we can extend the block
    if (compareIds(base, block.base) === 0 && offset == block.offsets[1]) {
        extendBlock(doc, idx, text, site)
        return
    }

    // If we are at the end of the block, insert a new block 
    const maxId = block.base.concat([block.offsets[1]-1])
    if (compareIds(maxId, id) < 0) {
        if (doc.DEBUG) {
            console.log("Inserting new block after index ", idx)
        }
        insertNewBlockAtIdx(doc, text, idx, site, base)
        return
    }

    // Insert in the middle of the block 
    // Find the point in the block where the new ID should be inserted
    const sp = findSplitPoint(block, id)
    splitAndInsertBlock(doc, idx, sp, text, site, base)
}

function deleteFromFront(doc: Document, idx: number, n: number) : Id[] {
    /* Delete n characters from the start of the block at idx */
    const block = doc.blocks[idx]!
    if (n > block.size) throw new Error("Number of chars to delete is more than size of the block")
    block.value = block.value.substring(n)
    block.size -= n
    // get delete indices from block 
    const delIndices : Id[] = []
    for (let i = 0; i < n; i++) {
        delIndices.push(block.base.concat([block.offsets[0] + i]))
    }
    // update block offsets 
    block.offsets[0] += n
    return delIndices
}

function deleteFromEnd(doc: Document, idx: number, n: number) : Id[] {  
    /* Delete n characters from the end of the block at idx */
    const block = doc.blocks[idx]!
    if (n > block.size) throw new Error("Number of chars to delete is more than size of the block")
    block.value = block.value.substring(0, block.value.length - n)
    block.size -= n
    // get delete indices from block 
    const delIndices : Id[] = []
    for (let i = 0; i < n; i++) {
        delIndices.push(block.base.concat([block.offsets[1] - 1 - i]))
    }
    // update block offsets 
    block.offsets[1] -= n
    return delIndices
}
    
function deleteAndSplit(doc: Document, idx: number, left: number, n: number) : Id[] {
    /* Delete n characters from the middle of the block at idx starting from "left" */
    // Split the block into two blocks 
    const block = doc.blocks[idx]!
    if (left + n > block.size) throw new Error("Number of chars to delete is more than size of the block")
    
    const block1 : Block = {
        base: block.base,
        offsets: [block.offsets[0], block.offsets[0] + left],
        value: block.value.substring(0, left),
        size: left,
        creator: block.creator
    }

    const block2 : Block = {
        base: block.base,
        offsets: [block.offsets[0] + left + n, block.offsets[1]],
        value: block.value.substring(left + n),
        size: block.size - left - n,
        creator: block.creator
    }

    // get delete indices from block
    const delIndices : Id[] = []
    for (let i = 0; i < n; i++) {
        delIndices.push(block.base.concat([block.offsets[0] + left + i]))
    }
    
    doc.blocks.splice(idx, 1, block1, block2)

    return delIndices
}

// from is inclusive, to is exclusive
function localDelete(doc: Document, from: number, to: number) : Operation {
    /* Collect all the IDs of the elements to be deleted */
    // Cases: 
    // 1. The entire block needs to be deleted 
    // 2. We are deleting at the end of the block
    // 3. We are deleting at the start of the block
    // 4. We are deleting in the middle of the block 
    // Find the index of "from"
    let numDelete = to - from
    let delIndices : Id[] = []

    while (numDelete > 0) {
        const posInfo = findDeleteIndex(doc, from)
        if (posInfo == null) {
            throw new Error("Cannot delete from an empty document")
        }

        let { idx, offset } = posInfo
        let startDel = from - offset 
        let endDel = to - offset 

        let indices : Id[] = []
        const blockSize = doc.blocks[idx]!.size        

        if (startDel == 0 && endDel >= blockSize) {
            // Case 1: If the entire block needs to be deleted
            // Delete the block 
            // Collect all indices corresponding to this block
            const block = doc.blocks[idx]!
            for (let i = 0; i < blockSize; i++) {
                indices.push(block.base.concat([block.offsets[0] + i]))
            }
            delIndices = delIndices.concat(indices)
            numDelete -= blockSize
            from += blockSize
            doc.blocks.splice(idx, 1)
        }
        else if (startDel == 0) {
            // Case 2: We are deleting some chars from the start of the block 
            indices = deleteFromFront(doc, idx, numDelete)
            delIndices = delIndices.concat(indices)
            numDelete = 0
        }
        else if (endDel >= doc.blocks[idx]!.size) {
            // Case 3: We are deleting at the end of the block
            indices = deleteFromEnd(doc, idx, blockSize - startDel)
            delIndices = delIndices.concat(indices)
            // Verification 
            assert(indices.length == blockSize - startDel, "Deleted indices length should be equal to number of chars deleted")
            numDelete -= blockSize - startDel
            from += delIndices.length
        } else {
            // Case 4: We are deleting in the middle of the block, split the block into two and delete the middle part
            indices = deleteAndSplit(doc, idx, startDel, numDelete)
            delIndices = delIndices.concat(indices)
            numDelete = 0
        }
    }

    return {
        type : 'del',
        ids: delIndices,
        offsets: [],
        payload: null,
        site: doc.state.replica,
        clock: doc.state.localClock
    }
}

function remoteDelete(doc: Document, op: Operation) : void {
    if (op.type !== 'del') throw new Error("Expected a delete operation")
    
    const delIds = op.ids

    /* Naive version of delete for now -- search each identifier one by one */ 
    for (let id of delIds) {
        const posInfo = searchBlockContainingId(doc, id)
        if (posInfo === null) return 

        const { idx, offset } = posInfo
        const block = doc.blocks[idx]!
        const minId = block.base.concat([block.offsets[0]])
        const maxId = block.base.concat([block.offsets[1]-1])

        if (compareIds(id, minId) == 0) {
            deleteFromFront(doc, idx, 1)
            if (block.size == 0) {
                doc.blocks.splice(idx, 1)
            }
        } else if (compareIds(id, maxId) == 0) {
            deleteFromEnd(doc, idx, 1)
            if (block.size == 0) {
                doc.blocks.splice(idx, 1)
            }
        } else {
            // ID is in the middle of the block, split the block and delete the middle part
            deleteAndSplit(doc, idx, offset, 1)
        }
    }
}

// // Simple tests 

// let doc1 = new Document(0)
// let doc2 = new Document(1)

// doc1.ins(0, "hi")
// doc1.mergeFrom(doc2)
// doc2.mergeFrom(doc1)

// // Delete tests 
// doc1.del(1,2)
// doc1.ins(1, "ello")
// doc2.ins(2, "world")

// doc1.mergeFrom(doc2)
// doc2.mergeFrom(doc1)

// assert.deepEqual(doc1.snapshot, doc2.snapshot)

// console.log("Tests passed!")
// console.log("Final document state: ", doc1.read())