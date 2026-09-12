-- Split the two clocks that messages.created_at used to carry.
--
-- created_at is now always the local persistence clock, so that
-- idx_messages_conversation_time orders a conversation by one clock
-- only. occurred_at holds the platform event time and is NULL for
-- outbound messages, which have no platform time of their own.

ALTER TABLE messages ADD COLUMN occurred_at INTEGER;

-- Rows written before this migration stored the platform event time in
-- created_at, so for inbound rows that value is the correct
-- occurred_at. Their true persistence time is not recoverable, so their
-- created_at stays on the platform clock.
UPDATE messages
SET occurred_at = created_at
WHERE direction = 'inbound';
