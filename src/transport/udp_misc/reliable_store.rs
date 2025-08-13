use std::{collections::VecDeque, time::Duration};

use std::time::Instant;
use tracing::debug;

use crate::{
    message::{QunetMessage, ReliabilityHeader},
    transport::{TransportError, exponential_moving_average},
};

struct UnackedRemoteMessage {
    message_id: u16,
    received_at: Instant,
}

struct UnackedLocalMessage {
    message_id: u16,
    message: QunetMessage,
    sent_at: Instant,
    retransmit_attempts: u8,
}

struct StoredOutOfOrderMessage {
    message_id: u16,
    message: QunetMessage,
    received_at: Instant,
}

pub(crate) struct ReliableStore {
    remote_unacked: VecDeque<UnackedRemoteMessage>,
    remote_out_of_order: VecDeque<StoredOutOfOrderMessage>,
    remote_delayed_queue: VecDeque<QunetMessage>,
    next_remote_id: u16,

    local_unacked: VecDeque<UnackedLocalMessage>,
    next_local_id: u16,

    avg_rtt_micros: u32,
}

impl ReliableStore {
    pub fn new() -> Self {
        Self {
            remote_unacked: VecDeque::new(),
            remote_out_of_order: VecDeque::new(),
            remote_delayed_queue: VecDeque::new(),
            next_remote_id: 1,
            local_unacked: VecDeque::new(),
            next_local_id: 1,
            avg_rtt_micros: 0,
        }
    }

    // Handling of incoming messages

    /// Handles an incoming reliable data message.
    /// If it's a duplicate message, `Ok(None)` is returned. If it's an out of order message, it will be stored for later processing, and `Ok(None)` is returned.
    /// If it's a valid message, it will be processed and `Ok(Some(message))` is returned.
    pub fn handle_incoming(
        &mut self,
        message: QunetMessage,
    ) -> Result<Option<QunetMessage>, TransportError> {
        let rel_header = match &message {
            QunetMessage::Data { reliability, .. } => {
                reliability.as_ref().expect("non-reliable message")
            }
            _ => panic!("non-data message given to reliable store"),
        };

        let msg_id = rel_header.message_id;

        self.process_acks(&rel_header.acks);

        if msg_id == 0 {
            // this message isn't reliable on its own, we don't need to store it
            return Ok(Some(message));
        }

        // a message is duplicate if its id is less than next expected and distance is small enough (because of wraparound)
        let is_duplicate = (msg_id < self.next_remote_id) && (self.next_remote_id - msg_id < 16384);

        // a message is out of order if it is not a duplicate and is not the next expected message
        let is_out_of_order = !is_duplicate && (msg_id != self.next_remote_id);

        if is_duplicate {
            // likely a duplicate message, our ACK might have been lost and we need to resend it
            debug!(
                "Duplicate message: expected {} but got {}, will re-ACK it",
                self.next_remote_id, msg_id
            );

            self.push_remote_duplicate(msg_id)?;

            return Ok(None);
        } else if is_out_of_order {
            // out of order message, store it for later processing
            debug!(
                "Out of order message: expected {} but got {}, storing for later",
                self.next_remote_id, msg_id
            );

            self.store_remote_out_of_order(message, msg_id)?;

            return Ok(None);
        }

        // otherwise, it's a valid message
        self.push_remote_unacked()?;

        Ok(Some(message))
    }

    fn store_remote_out_of_order(
        &mut self,
        message: QunetMessage,
        message_id: u16,
    ) -> Result<(), TransportError> {
        if self.remote_out_of_order.len() >= 16 {
            return Err(TransportError::TooUnreliable);
        }

        // we want to maintain a sorted (ascending) order of out of order messages by their IDs,
        // so use binary search to find the right position.
        // this also serves as a way to check if the message is already in the queue
        let pos = match self.remote_out_of_order.binary_search_by_key(&message_id, |m| m.message_id)
        {
            Ok(_) => {
                debug!("Message is already in the out of order queue, ignoring");
                return Ok(());
            }

            Err(pos) => pos,
        };

        let msg = StoredOutOfOrderMessage {
            message_id,
            message,
            received_at: Instant::now(),
        };

        self.remote_out_of_order.insert(pos, msg);

        Ok(())
    }

    fn push_remote_unacked(&mut self) -> Result<(), TransportError> {
        let id = self.next_remote_id;
        self.next_remote_id = self.next_remote_id.wrapping_add(1);

        if self.next_remote_id == 0 {
            self.next_remote_id = 1; // wrap around to 1
        }

        debug!("Received a reliable message with ID {id}");

        self.push_remote_unacked_with_id(id, false)
    }

    fn push_remote_duplicate(&mut self, message_id: u16) -> Result<(), TransportError> {
        self.push_remote_unacked_with_id(message_id, true)
    }

    fn push_remote_unacked_with_id(
        &mut self,
        message_id: u16,
        dupe: bool,
    ) -> Result<(), TransportError> {
        // if this is a duplicate message, check if it's already in the unacked queue
        if dupe && self.remote_unacked.iter().any(|m| m.message_id == message_id) {
            return Ok(());
        }

        if self.remote_unacked.len() >= 64 {
            debug!("Remote unacked queue is full, terminating connection");
            return Err(TransportError::TooUnreliable);
        }

        self.remote_unacked.push_back(UnackedRemoteMessage {
            message_id,
            received_at: Instant::now(),
        });

        // maybe restore some out of order message if it's the right time
        self.maybe_restore_remote()?;

        Ok(())
    }

    fn maybe_restore_remote(&mut self) -> Result<(), TransportError> {
        // pop if the id matches or if the message has been there for too long
        let do_pop = self.remote_out_of_order.front().is_some_and(|msg| {
            msg.message_id == self.next_remote_id
                || msg.received_at.elapsed() > Duration::from_secs(3)
        });

        if do_pop {
            let msg_id = self.remote_out_of_order.front().unwrap().message_id;

            if msg_id == self.next_remote_id {
                if self.remote_delayed_queue.len() >= 16 {
                    return Err(TransportError::TooUnreliable);
                }

                let msg = self.remote_out_of_order.pop_front().unwrap();
                self.remote_delayed_queue.push_back(msg.message);
                self.push_remote_unacked()?;
            } else {
                // this message is too old, we can just drop it
                debug!("Dropping out of order message with ID {} because it's too old", msg_id);
                self.remote_out_of_order.pop_front();

                // maybe restore another message
                self.maybe_restore_remote()?;
            }
        }

        Ok(())
    }

    #[inline]
    pub fn pop_delayed_message(&mut self) -> Option<QunetMessage> {
        self.remote_delayed_queue.pop_front()
    }

    // Handling of outgoing messages

    /// Returns the next message ID to use for reliable outgoing messages.
    #[inline]
    pub fn next_message_id(&mut self) -> u16 {
        let id = self.next_local_id;
        self.next_local_id = self.next_local_id.wrapping_add(1);

        if self.next_local_id == 0 {
            self.next_local_id = 1; // wrap around to 1
        }

        id
    }

    /// Modifies the reliability header of the given message to add ACKs for unacked messages from the remote.
    pub fn set_outgoing_acks(&mut self, hdr: &mut ReliabilityHeader) {
        hdr.acks.clear();

        while hdr.acks.len() < hdr.acks.capacity()
            && let Some(msg) = self.remote_unacked.pop_front()
        {
            let _ = hdr.acks.push(msg.message_id);
        }
    }

    /// Stores a local message for potential retransmission. It must be a reliable data message.
    pub fn push_local_unacked(&mut self, message: QunetMessage) -> Result<(), TransportError> {
        let rel_header = match &message {
            QunetMessage::Data { reliability, .. } => {
                reliability.as_ref().expect("non-reliable message")
            }
            _ => panic!("non-data message given to reliable store"),
        };

        let msg_id = rel_header.message_id;

        if msg_id == 0 {
            // this message isn't reliable on its own, we don't need to store it
            return Ok(());
        }

        if self.local_unacked.len() >= 32 {
            debug!("Local unacked queue is full, terminating connection");
            return Err(TransportError::TooUnreliable);
        }

        self.local_unacked.push_back(UnackedLocalMessage {
            message_id: msg_id,
            message,
            sent_at: Instant::now(),
            retransmit_attempts: 0,
        });

        Ok(())
    }

    /// Checks if any messages need to be retransmitted. If this function returns `true`,
    /// it updates the sent time of the message to be resent, and that message can then be obtained
    /// via `get_retransmit_message()`.
    pub fn maybe_retransmit(&mut self) -> Result<bool, TransportError> {
        // the messages are sorted by sent time, so we can just check the first one
        if let Some(mut msg) = self.local_unacked.pop_front() {
            let lim = self.calc_retransmission_deadline(msg.retransmit_attempts as usize);

            let elapsed = msg.sent_at.elapsed();

            if elapsed >= lim {
                if msg.retransmit_attempts >= 10 {
                    debug!(
                        "Message with ID {} has been retransmitted too many times, bailing out",
                        msg.message_id
                    );

                    return Err(TransportError::TooUnreliable);
                } else {
                    debug!("Retransmitting message with ID {} after {:?}", msg.message_id, elapsed);
                }

                // update the sent time
                msg.sent_at = Instant::now();
                msg.retransmit_attempts += 1;
                self.local_unacked.push_back(msg); // put it back at the end

                return Ok(true);
            } else {
                // not yet time to retransmit, put it back
                self.local_unacked.push_front(msg);
            }
        }

        Ok(false)
    }

    /// Only call this function if `maybe_retransmit()` returned `true`.
    pub fn get_retransmit_message(&self) -> Option<&QunetMessage> {
        self.local_unacked.back().map(|m| &m.message)
    }

    /// Returns whether there are any unacked remote messages that must be acknowledged as soon as possible.
    pub fn has_urgent_outgoing_acks(&self) -> bool {
        let lim = self.calc_ack_deadline();

        self.remote_unacked.iter().any(|msg| msg.received_at.elapsed() > lim)
    }

    #[inline]
    fn calc_retransmission_deadline(&self, nth_attempt: usize) -> Duration {
        let mut rtt = Duration::from_micros(self.avg_rtt_micros as u64);

        if rtt.is_zero() {
            rtt = Duration::from_millis(250);
        }

        let ack_delay = self.calc_ack_deadline();

        let sum = (rtt + ack_delay).as_secs_f64();

        Duration::from_secs_f64(sum * 1.5 + (nth_attempt as f64 * 0.075))
    }

    #[inline]
    const fn calc_ack_deadline(&self) -> Duration {
        Duration::from_millis(100)
    }

    fn process_acks(&mut self, acks: &[u16]) {
        for ack in acks {
            debug!("Remote acknowledged message with ID {}", ack);

            // find the message in the local unacked queue
            let pos = self.local_unacked.iter().position(|m| m.message_id == *ack);

            match pos {
                Some(pos) => {
                    self.update_rtt(self.local_unacked[pos].sent_at.elapsed());
                    self.local_unacked.remove(pos);
                }

                None => {
                    debug!("Ignoring ACK for unknown message ID {}", ack);
                }
            }
        }
    }

    #[inline]
    fn update_rtt(&mut self, rtt: Duration) {
        let micros: u32 = rtt.as_micros().try_into().unwrap_or(u32::MAX);

        self.avg_rtt_micros = exponential_moving_average(micros, self.avg_rtt_micros, 0.35) as u32;
    }

    pub fn until_timer_expiry(&self) -> Duration {
        // the timer expires in 2 cases:
        // 1. to retransmit an unacked local message
        // 2. to send an ACK for an unacked remote message

        // we go through all messages and return the minimum expiry time

        let mut min_expiry = Duration::from_secs(u64::MAX);

        // fast return
        if self.local_unacked.is_empty() && self.remote_unacked.is_empty() {
            return min_expiry;
        }

        let ack_delay = self.calc_ack_deadline();
        let now = Instant::now();

        for msg in &self.local_unacked {
            let retrans_delay = self.calc_retransmission_deadline(msg.retransmit_attempts as usize);

            let dur = retrans_delay.saturating_sub(now.duration_since(msg.sent_at));
            if dur < min_expiry {
                min_expiry = dur;
            }
        }

        for msg in &self.remote_unacked {
            let dur = ack_delay.saturating_sub(now.duration_since(msg.received_at));
            if dur < min_expiry {
                min_expiry = dur;
            }
        }

        min_expiry
    }
}
