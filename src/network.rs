use std::sync::Arc;
use std::collections::HashMap;

use crate::operation::{WireOperation};
use crate::document::Document;

#[derive(Clone, Debug)]
pub struct Network {
    pub documents: Vec<Document>,
    inboxes: Vec<HashMap<u32, Vec<Arc<WireOperation>>>>,
    site_index: HashMap<u32, usize>,
}

impl Network {
    pub fn new(docs: Vec<Document>) -> Self {
        let mut site_index = HashMap::with_capacity(docs.len());
        for (i, doc) in docs.iter().enumerate() {
            site_index.insert(doc.site_id(), i);
        }
        let n = docs.len();
        Network {
            documents: docs,
            inboxes: vec![HashMap::new(); n],
            site_index,
        }
    }

    pub fn add_peer(&mut self, doc: Document) {
        let idx = self.documents.len();
        self.site_index.insert(doc.site_id(), idx);
        self.documents.push(doc);
        self.inboxes.push(HashMap::new());
    }

    pub fn broadcast(&mut self, op: WireOperation, sender: u32) {
        let op = Arc::new(op);
        for (i, inbox) in self.inboxes.iter_mut().enumerate() {
            if self.documents[i].site_id() != sender {
                inbox.entry(sender)
                    .or_insert_with(Vec::new)
                    .push(Arc::clone(&op));
            }
        }
    }

    pub fn sync_from(&mut self, into: u32, from: u32) {
        let idx = self.site_index[&into];
        // Take the entire sender queue in O(1) — no scanning, no partition
        let to_apply = self.inboxes[idx]
            .remove(&from)
            .unwrap_or_default();
        for op in to_apply {
            self.documents[idx].apply_remote_op(&op);
        }
    }

    pub fn sync_all(&mut self) {
        // Collect all (recipient_idx, sender_site) pairs that have pending ops
        let work: Vec<(u32, u32)> = self.inboxes.iter().enumerate()
            .flat_map(|(i, inbox)| {
                let into = self.documents[i].site_id();
                inbox.keys().map(move |&from| (into, from)).collect::<Vec<_>>()
            })
            .collect();
        for (into, from) in work {
            self.sync_from(into, from);
        }
    }

    pub fn index_of(&self, site: u32) -> usize {
        self.site_index[&site]
    }

    pub fn reset(&mut self) {
        for doc in &mut self.documents {
            doc.reset();
        }
        for inbox in &mut self.inboxes {
            inbox.clear();
        }
    }
}