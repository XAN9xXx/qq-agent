CREATE UNIQUE INDEX uq_messages_inbound_source_event
ON messages(source_event_key)
WHERE direction = 'inbound'
  AND source_event_key IS NOT NULL;
