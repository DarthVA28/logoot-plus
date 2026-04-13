use wasm_bindgen::prelude::*;
// Assuming your core CRDT logic is in a module named `crdt` or similar.
use crate::Document;

#[wasm_bindgen(js_name = Document)]
pub struct WasmDocument {
    inner: Document, 
}

#[wasm_bindgen(js_class = Document)]
impl WasmDocument {
    #[wasm_bindgen(constructor)]
    pub fn new(id: u32) -> WasmDocument {
        WasmDocument {
            inner: Document::new(id),
        }
    }

    pub fn ins(&mut self, pos: usize, text: String) {
        self.inner.ins(pos, text);
    }

    pub fn del(&mut self, from: usize, to: usize) {
        self.inner.del(from, to);
    }

    pub fn read(&mut self) -> String {
        self.inner.read()
    }

    #[wasm_bindgen(js_name = mergeFrom)]
    pub fn merge_from(&mut self, other: &WasmDocument) {
        self.inner.merge_from(&other.inner);
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }

    // Exposing internal block state for your console.log debugging
    #[wasm_bindgen(js_name = getDebugBlocks)]
    pub fn get_debug_blocks(&self) -> String {
        format!("{:?}", self.inner.blocks) // Assuming BlockTree derives Debug
    }
}