use std::{
    sync::atomic::{AtomicU16, Ordering},
    time::{Duration, Instant},
};

use nohash_hasher::IntMap;
use tracing::debug;

use crate::{
    buffers::multi_buffer_pool::MultiBufferPool,
    message::{BufferKind, CompressionHeader, DataMessageKind, QunetMessage, ReliabilityHeader},
    transport::TransportError,
};

struct Fragment {
    index: u16,
    data: BufferKind,
}

struct Message {
    received_at: Instant,
    total_fragments: usize, // -1 means unknown
    fragments: Vec<Fragment>,
    compression_header: Option<CompressionHeader>,
    reliability_header: Option<ReliabilityHeader>,
}

pub(crate) struct FragmentStore {
    messages: IntMap<u16, Message>,
    next_message_id: AtomicU16,
}

impl FragmentStore {
    pub fn new() -> Self {
        Self {
            messages: IntMap::default(),
            next_message_id: AtomicU16::new(1),
        }
    }

    #[inline]
    pub fn next_message_id(&self) -> u16 {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Accepts incoming message fragment. If a whole message is ready, it will be reassembled and returned, otherwise None is returned.
    pub fn process_fragment(
        &mut self,
        message: QunetMessage,
        buffer_pool: &MultiBufferPool,
    ) -> Result<Option<QunetMessage>, TransportError> {
        let (data, frag_header, rel_header, comp_header) = match message {
            QunetMessage::Data { kind, reliability, compression } => match kind {
                DataMessageKind::Fragment { header, data } => {
                    (data, header, reliability, compression)
                }
                _ => panic!("Expected a fragmented message"),
            },
            _ => panic!("Expected a data message with fragments"),
        };

        // get or create the message entry
        let message_id = frag_header.message_id;
        let already_messages = self.messages.len();

        let ent = self.messages.entry(message_id);
        if matches!(ent, std::collections::hash_map::Entry::Vacant(_)) && already_messages == 16 {
            return Err(TransportError::TooManyPendingFragments);
        }

        let ent = ent.or_insert_with(|| Message {
            received_at: Instant::now(),
            total_fragments: usize::MAX,
            fragments: Vec::new(),
            compression_header: None,
            reliability_header: None,
        });

        // check if this is a duplicate fragment
        if ent.fragments.iter().any(|f| f.index == frag_header.fragment_index) {
            return Ok(None); // duplicate fragment, ignore it
        }

        // if this is the final fragment, we can set the total fragments count
        if frag_header.last_fragment {
            ent.total_fragments = frag_header.fragment_index as usize + 1;
        }

        // add the fragment to the message
        ent.fragments.push(Fragment {
            index: frag_header.fragment_index,
            data,
        });

        // if some headers are present, store them
        if let Some(comp_header) = comp_header {
            ent.compression_header = Some(comp_header);
        }

        if let Some(rel_header) = rel_header {
            ent.reliability_header = Some(rel_header);
        }

        // if this message is complete, reassemble it
        if ent.total_fragments == ent.fragments.len() {
            let reassembled = Self::reassemble(ent, buffer_pool);
            self.messages.remove(&message_id); // remove the message from the store, even if reassembly failed
            self.cleanup();

            Ok(Some(reassembled?))
        } else {
            // not enough fragments yet, run cleanup and return None
            self.cleanup();
            Ok(None)
        }
    }

    fn cleanup(&mut self) {
        // remove messages that are too old
        const MAX_AGE: Duration = Duration::from_secs(10);

        let now = Instant::now();

        self.messages.retain(|id, msg| {
            if now.duration_since(msg.received_at) > MAX_AGE {
                debug!("Removing stale fragmented message (ID {})", *id);
                false // remove this message
            } else {
                true // keep this message
            }
        });
    }

    fn reassemble(
        message: &mut Message,
        buffer_pool: &MultiBufferPool,
    ) -> Result<QunetMessage, TransportError> {
        // sort fragments by index
        message.fragments.sort_by_key(|f| f.index);

        // calculate total size and see if there's any skips
        let mut total_size = 0;

        for (expected_index, frag) in message.fragments.iter().enumerate() {
            if frag.index as usize != expected_index {
                return Err(TransportError::DefragmentationError);
            }

            total_size += frag.data.len();
        }

        let mut data = match buffer_pool.get_busy_loop(total_size) {
            Some(buf) => BufferKind::new_pooled(buf),

            None => BufferKind::new_heap(total_size),
        };

        for frag in &message.fragments {
            assert!(data.append_bytes(&frag.data), "Fragment data size mismatch");
        }

        Ok(QunetMessage::Data {
            kind: DataMessageKind::Regular { data },
            reliability: message.reliability_header.take(),
            compression: message.compression_header.take(),
        })
    }
}
