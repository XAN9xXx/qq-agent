CREATE TABLE outbound_attempts (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    source_event_key TEXT,
    text TEXT NOT NULL,

    status TEXT NOT NULL CHECK (
        status IN (
            'pending',
            'sent',
            'not_dispatched',
            'explicit_failure',
            'unknown_outcome'
        )
    ),

    external_message_id TEXT,
    retcode INTEGER,

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    FOREIGN KEY(conversation_id)
        REFERENCES conversations(id),

    FOREIGN KEY(source_event_key)
        REFERENCES inbound_events(dedup_key)
);

CREATE INDEX idx_outbound_attempts_conversation_time
ON outbound_attempts(
    conversation_id,
    created_at DESC
);

CREATE INDEX idx_outbound_attempts_status
ON outbound_attempts(
    status,
    updated_at
);
