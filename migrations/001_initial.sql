PRAGMA foreign_keys = ON;

CREATE TABLE actors (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    external_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,

    UNIQUE(platform, external_id)
);

CREATE TABLE scopes (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    scope_type TEXT NOT NULL
        CHECK (scope_type IN ('private', 'group')),
    external_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,

    UNIQUE(platform, scope_type, external_id)
);

CREATE TABLE conversations (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,

    context_reset_at INTEGER,

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    FOREIGN KEY(scope_id) REFERENCES scopes(id),
    FOREIGN KEY(actor_id) REFERENCES actors(id),

    UNIQUE(scope_id, actor_id)
);

CREATE TABLE inbound_events (
    dedup_key TEXT PRIMARY KEY,

    event_type TEXT NOT NULL,
    external_message_id TEXT,

    scope_id TEXT,
    actor_id TEXT,

    occurred_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL,

    state TEXT NOT NULL
        CHECK (
            state IN (
                'received',
                'processing',
                'completed',
                'failed',
                'abandoned'
            )
        ),

    state_updated_at INTEGER NOT NULL,

    raw_json TEXT NOT NULL,

    FOREIGN KEY(scope_id) REFERENCES scopes(id),
    FOREIGN KEY(actor_id) REFERENCES actors(id)
);

CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    conversation_id TEXT NOT NULL,

    direction TEXT NOT NULL
        CHECK (direction IN ('inbound', 'outbound')),

    sender_actor_id TEXT,

    external_message_id TEXT,
    source_event_key TEXT,

    text TEXT NOT NULL,
    segments_json TEXT,

    created_at INTEGER NOT NULL,

    FOREIGN KEY(conversation_id)
        REFERENCES conversations(id),

    FOREIGN KEY(sender_actor_id)
        REFERENCES actors(id),

    FOREIGN KEY(source_event_key)
        REFERENCES inbound_events(dedup_key)
);

CREATE INDEX idx_conversations_scope_actor
ON conversations(scope_id, actor_id);

CREATE INDEX idx_inbound_events_state_updated
ON inbound_events(state, state_updated_at);

CREATE INDEX idx_inbound_events_external_message
ON inbound_events(external_message_id);

CREATE INDEX idx_messages_conversation_time
ON messages(conversation_id, created_at DESC, id DESC);

CREATE INDEX idx_messages_external_message
ON messages(external_message_id);
