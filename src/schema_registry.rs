use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct SchemaArtifact {
    pub key: &'static str,
    pub version: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaState {
    pub version: i64,
    pub last_updated_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl SchemaState {
    fn with_note(version: i64, note: Option<&str>) -> Self {
        Self {
            version,
            last_updated_ms: Utc::now().timestamp_millis(),
            note: note.map(|s| s.to_string()),
        }
    }
}

pub struct SchemaRegistry<'conn> {
    conn: &'conn Connection,
}

impl<'conn> SchemaRegistry<'conn> {
    pub fn new(conn: &'conn Connection) -> Result<Self> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn current_state(
        &self,
        artifact: &SchemaArtifact,
    ) -> Result<Option<SchemaState>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?",
                [artifact.key],
                |row| row.get(0),
            )
            .optional()?;

        raw.map(|value| Self::parse_state(&value)).transpose()
    }

    pub fn needs_update(&self, artifact: &SchemaArtifact) -> Result<bool> {
        let state = self.current_state(artifact)?;
        Ok(!matches!(
            state,
            Some(existing) if existing.version == artifact.version
        ))
    }

    pub fn mark_current(
        &self,
        artifact: &SchemaArtifact,
        note: Option<&str>,
    ) -> Result<()> {
        let state = SchemaState::with_note(artifact.version, note);
        let serialized = serde_json::to_string(&state)?;
        self.conn.execute(
            "INSERT INTO metadata (key, value)
             VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![artifact.key, serialized],
        )?;
        Ok(())
    }

    fn parse_state(raw: &str) -> Result<SchemaState> {
        if raw.trim_start().starts_with('{') {
            Ok(serde_json::from_str(raw)?)
        } else {
            let version = raw.trim().parse::<i64>()?;
            Ok(SchemaState {
                version,
                last_updated_ms: 0,
                note: None,
            })
        }
    }
}
