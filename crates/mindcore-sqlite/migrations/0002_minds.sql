-- The Mind registry: one row per Mind. A Mind is one memory space (`memory_spaces`), so the row's
-- key is the space's id and there is no second Mind id. The row binds the space to the knowledge
-- collection the Mind's entries and carrier live in, and to the carrier entry that holds its
-- current judgment. It confers no access: every operation still admits the caller under its own
-- live grants in the Mind's space.
CREATE TABLE minds (
    space_id             TEXT PRIMARY KEY CHECK (length(space_id) > 0),
    knowledge_collection TEXT NOT NULL CHECK (length(knowledge_collection) > 0),
    carrier_entry_id     TEXT NOT NULL CHECK (length(carrier_entry_id) > 0),
    FOREIGN KEY (space_id) REFERENCES memory_spaces(space_id),
    FOREIGN KEY (knowledge_collection, carrier_entry_id)
        REFERENCES knowledge_entries(collection, entry_id)
) WITHOUT ROWID;
