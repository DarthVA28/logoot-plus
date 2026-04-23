/* Network layer for communication in Operation Based CRDT */

use std::sync::Arc;

use crate::operation::{Operation};
use crate::document::Document;

#[derive(Clone, Debug)]
pub struct Network {
    // List of documents 
    pub documents: Vec<Document>,
    // Received messages
    pub inboxes: Vec<Vec<Arc<Operation>>>
}

impl Network {
    pub fn new(docs: Vec<Document>) -> Self {
        let n = docs.len();
        Network { documents: docs, inboxes: vec![vec![]; n] }
    }

    pub fn add_peer(&mut self, doc: Document) {
        self.documents.push(doc);
        self.inboxes.push(vec![]);
    }

    pub fn broadcast(&mut self, op: Operation, sender: u32) {
        let op = Arc::new(op);
        for (i, inbox) in self.inboxes.iter_mut().enumerate() {
            if self.documents[i].site_id() != sender {
                // println!("Adding op from site {} to inbox of site {}", sender, self.documents[i].site_id());
                inbox.push(Arc::clone(&op));  
            }
        }
    }

    pub fn sync_from(&mut self, into: u32, from: u32) {
        let idx = self.index_of(into);
        let mut remaining = vec![];
        let mut to_apply = vec![];

        // println!("Syncing from site {} into site {}", from, into);

        for op in self.inboxes[idx].drain(..) {
            if op.site == from {
                // Kept silent in hot path to avoid benchmark distortion from stdout overhead.
                to_apply.push(op);
            } else {
                remaining.push(op);
            }
        }

        self.inboxes[idx] = remaining;
        for op in to_apply {
            self.documents[idx].apply_op(&op);
        }
    }

    pub fn sync_all(&mut self) {
        let sites: Vec<u32> = self.documents.iter().map(|d| d.site_id()).collect();
        for i in 0..self.documents.len() {
            for j in 0..self.documents.len() {
                if i != j {
                    self.sync_from(sites[i], sites[j]);
                }
            }
        }
    }

    pub fn index_of(&self, site: u32) -> usize {
        self.documents
            .iter()
            .position(|d| d.site_id() == site)
            .expect("site not found in network")
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