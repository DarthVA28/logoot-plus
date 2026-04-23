use wasm_bindgen::prelude::*;
use crate::{Document, LogootSplitSystem};
use crate::network::Network;

#[wasm_bindgen(js_name = LogootSplitSystem)]
pub struct WasmLogootSplitSystem {
    inner: LogootSplitSystem,
}

#[wasm_bindgen(js_class = LogootSplitSystem)]
impl WasmLogootSplitSystem {
    #[wasm_bindgen(constructor)]
    pub fn new(n: usize) -> WasmLogootSplitSystem {
        let docs = (0..n as u32).map(Document::new).collect();
        WasmLogootSplitSystem {
            inner: LogootSplitSystem { network: Network::new(docs) }
        }
    }

    pub fn ins(&mut self, doc_id: u32, pos: usize, text: String) {
        self.inner.ins(doc_id, pos, text);
    }

    pub fn del(&mut self, doc_id: u32, from: usize, to: usize) {
        self.inner.del(doc_id, from, to);
    }

    pub fn read(&mut self, doc_id: u32) -> String {
        self.inner.read(doc_id)
    }

    #[wasm_bindgen(js_name = syncFrom)]
    pub fn sync_from(&mut self, into: u32, from: u32) {
        self.inner.merge_from(into, from);
    }

    #[wasm_bindgen(js_name = getDebugBlocks)]
    pub fn get_debug_blocks(&self, doc_id: u32) -> String {
        let idx = self.inner.network.index_of(doc_id);
        format!("{:?}", self.inner.network.documents[idx].blocks)
    }
    
    #[wasm_bindgen(js_name = reset)]
    pub fn reset(&mut self) {
        self.inner.reset();
    }
}