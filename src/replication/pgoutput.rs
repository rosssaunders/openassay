use byteorder::{BigEndian, ByteOrder};

use crate::replication::ReplicationError;

#[derive(Debug, Clone, PartialEq)]
pub enum PgOutputMessage {
    Begin(BeginMessage),
    Commit(CommitMessage),
    Relation(RelationMessage),
    Insert(InsertMessage),
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Truncate(TruncateMessage),
    Origin(OriginMessage),
    Type(TypeMessage),
    Message(LogicalMessage),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeginMessage {
    pub final_lsn: u64,
    pub commit_timestamp: i64,
    pub xid: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitMessage {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationMessage {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationColumn {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertMessage {
    pub relation_id: u32,
    pub new_tuple: TupleData,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OldTupleKind {
    Key,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMessage {
    pub relation_id: u32,
    pub old_tuple: Option<(OldTupleKind, TupleData)>,
    pub new_tuple: TupleData,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteMessage {
    pub relation_id: u32,
    pub old_tuple: (OldTupleKind, TupleData),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TruncateMessage {
    pub relation_ids: Vec<u32>,
    pub cascade: bool,
    pub restart_identity: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OriginMessage {
    pub lsn: u64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypeMessage {
    pub type_id: u32,
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalMessage {
    pub flags: u8,
    pub lsn: u64,
    pub prefix: String,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TupleData {
    pub columns: Vec<TupleColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TupleColumn {
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

pub fn decode_message(bytes: &[u8]) -> Result<PgOutputMessage, ReplicationError> {
    let mut cursor = Cursor::new(bytes);
    let tag = cursor.read_u8()?;
    match tag {
        b'B' => Ok(PgOutputMessage::Begin(decode_begin(&mut cursor)?)),
        b'C' => Ok(PgOutputMessage::Commit(decode_commit(&mut cursor)?)),
        b'R' => Ok(PgOutputMessage::Relation(decode_relation(&mut cursor)?)),
        b'I' => Ok(PgOutputMessage::Insert(decode_insert(&mut cursor)?)),
        b'U' => Ok(PgOutputMessage::Update(decode_update(&mut cursor)?)),
        b'D' => Ok(PgOutputMessage::Delete(decode_delete(&mut cursor)?)),
        b'T' => Ok(PgOutputMessage::Truncate(decode_truncate(&mut cursor)?)),
        b'O' => Ok(PgOutputMessage::Origin(decode_origin(&mut cursor)?)),
        b'Y' => Ok(PgOutputMessage::Type(decode_type(&mut cursor)?)),
        b'M' => Ok(PgOutputMessage::Message(decode_logical_message(&mut cursor)?)),
        _ => Err(ReplicationError {
            message: format!("unknown pgoutput tag {}", tag as char),
        }),
    }
}

pub fn decode_stream(bytes: &[u8]) -> Result<Vec<PgOutputMessage>, ReplicationError> {
    let mut cursor = Cursor::new(bytes);
    let mut messages = Vec::new();
    while cursor.remaining() > 0 {
        let start = cursor.offset();
        let tag = cursor.read_u8()?;
        let message = match tag {
            b'B' => PgOutputMessage::Begin(decode_begin(&mut cursor)?),
            b'C' => PgOutputMessage::Commit(decode_commit(&mut cursor)?),
            b'R' => PgOutputMessage::Relation(decode_relation(&mut cursor)?),
            b'I' => PgOutputMessage::Insert(decode_insert(&mut cursor)?),
            b'U' => PgOutputMessage::Update(decode_update(&mut cursor)?),
            b'D' => PgOutputMessage::Delete(decode_delete(&mut cursor)?),
            b'T' => PgOutputMessage::Truncate(decode_truncate(&mut cursor)?),
            b'O' => PgOutputMessage::Origin(decode_origin(&mut cursor)?),
            b'Y' => PgOutputMessage::Type(decode_type(&mut cursor)?),
            b'M' => PgOutputMessage::Message(decode_logical_message(&mut cursor)?),
            _ => {
                return Err(ReplicationError {
                    message: format!("unknown pgoutput tag {}", tag as char),
                })
            }
        };
        if cursor.offset() == start + 1 {
            return Err(ReplicationError {
                message: "pgoutput decoder did not advance".to_string(),
            });
        }
        messages.push(message);
    }
    Ok(messages)
}

fn decode_begin(cursor: &mut Cursor<'_>) -> Result<BeginMessage, ReplicationError> {
    Ok(BeginMessage {
        final_lsn: cursor.read_u64()?,
        commit_timestamp: cursor.read_i64()?,
        xid: cursor.read_u32()?,
    })
}

fn decode_commit(cursor: &mut Cursor<'_>) -> Result<CommitMessage, ReplicationError> {
    Ok(CommitMessage {
        flags: cursor.read_u8()?,
        commit_lsn: cursor.read_u64()?,
        end_lsn: cursor.read_u64()?,
        commit_timestamp: cursor.read_i64()?,
    })
}

fn decode_relation(cursor: &mut Cursor<'_>) -> Result<RelationMessage, ReplicationError> {
    let relation_id = cursor.read_u32()?;
    let namespace = cursor.read_cstring()?;
    let name = cursor.read_cstring()?;
    let replica_identity = cursor.read_u8()?;
    let column_count = cursor.read_i16()? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(RelationColumn {
            flags: cursor.read_u8()?,
            name: cursor.read_cstring()?,
            type_oid: cursor.read_u32()?,
            type_modifier: cursor.read_i32()?,
        });
    }
    Ok(RelationMessage {
        relation_id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(cursor: &mut Cursor<'_>) -> Result<InsertMessage, ReplicationError> {
    let relation_id = cursor.read_u32()?;
    let tuple_tag = cursor.read_u8()?;
    if tuple_tag != b'N' {
        return Err(ReplicationError {
            message: "INSERT message missing new tuple".to_string(),
        });
    }
    let new_tuple = decode_tuple(cursor)?;
    Ok(InsertMessage {
        relation_id,
        new_tuple,
    })
}

fn decode_update(cursor: &mut Cursor<'_>) -> Result<UpdateMessage, ReplicationError> {
    let relation_id = cursor.read_u32()?;
    let mut old_tuple = None;
    let next_tag = cursor.peek_u8()?;
    if next_tag == b'K' || next_tag == b'O' {
        let kind = if cursor.read_u8()? == b'K' {
            OldTupleKind::Key
        } else {
            OldTupleKind::Full
        };
        let tuple = decode_tuple(cursor)?;
        old_tuple = Some((kind, tuple));
    }
    let new_tag = cursor.read_u8()?;
    if new_tag != b'N' {
        return Err(ReplicationError {
            message: "UPDATE message missing new tuple".to_string(),
        });
    }
    let new_tuple = decode_tuple(cursor)?;
    Ok(UpdateMessage {
        relation_id,
        old_tuple,
        new_tuple,
    })
}

fn decode_delete(cursor: &mut Cursor<'_>) -> Result<DeleteMessage, ReplicationError> {
    let relation_id = cursor.read_u32()?;
    let kind = match cursor.read_u8()? {
        b'K' => OldTupleKind::Key,
        b'O' => OldTupleKind::Full,
        _ => {
            return Err(ReplicationError {
                message: "DELETE message missing old tuple".to_string(),
            })
        }
    };
    let tuple = decode_tuple(cursor)?;
    Ok(DeleteMessage {
        relation_id,
        old_tuple: (kind, tuple),
    })
}

fn decode_truncate(cursor: &mut Cursor<'_>) -> Result<TruncateMessage, ReplicationError> {
    let count = cursor.read_u32()? as usize;
    let options = cursor.read_u8()?;
    let mut relation_ids = Vec::with_capacity(count);
    for _ in 0..count {
        relation_ids.push(cursor.read_u32()?);
    }
    Ok(TruncateMessage {
        relation_ids,
        cascade: options & 0x01 != 0,
        restart_identity: options & 0x02 != 0,
    })
}

fn decode_origin(cursor: &mut Cursor<'_>) -> Result<OriginMessage, ReplicationError> {
    Ok(OriginMessage {
        lsn: cursor.read_u64()?,
        name: cursor.read_cstring()?,
    })
}

fn decode_type(cursor: &mut Cursor<'_>) -> Result<TypeMessage, ReplicationError> {
    Ok(TypeMessage {
        type_id: cursor.read_u32()?,
        namespace: cursor.read_cstring()?,
        name: cursor.read_cstring()?,
    })
}

fn decode_logical_message(cursor: &mut Cursor<'_>) -> Result<LogicalMessage, ReplicationError> {
    let flags = cursor.read_u8()?;
    let lsn = cursor.read_u64()?;
    let prefix = cursor.read_cstring()?;
    let len = cursor.read_i32()? as usize;
    let content = cursor.read_bytes(len)?;
    Ok(LogicalMessage {
        flags,
        lsn,
        prefix,
        content,
    })
}

fn decode_tuple(cursor: &mut Cursor<'_>) -> Result<TupleData, ReplicationError> {
    let column_count = cursor.read_i16()? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let kind = cursor.read_u8()?;
        let column = match kind {
            b'n' => TupleColumn::Null,
            b'u' => TupleColumn::Unchanged,
            b't' => {
                let len = cursor.read_i32()? as usize;
                let bytes = cursor.read_bytes(len)?;
                let text = String::from_utf8(bytes).map_err(|err| ReplicationError {
                    message: err.to_string(),
                })?;
                TupleColumn::Text(text)
            }
            b'b' => {
                let len = cursor.read_i32()? as usize;
                TupleColumn::Binary(cursor.read_bytes(len)?)
            }
            _ => {
                return Err(ReplicationError {
                    message: "tuple data has unknown column kind".to_string(),
                })
            }
        };
        columns.push(column);
    }
    Ok(TupleData { columns })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn peek_u8(&self) -> Result<u8, ReplicationError> {
        self.bytes.get(self.offset).copied().ok_or_else(|| ReplicationError {
            message: "unexpected end of pgoutput message".to_string(),
        })
    }

    fn read_u8(&mut self) -> Result<u8, ReplicationError> {
        let out = self.peek_u8()?;
        self.offset += 1;
        Ok(out)
    }

    fn read_i16(&mut self) -> Result<i16, ReplicationError> {
        let bytes = self.read_bytes(2)?;
        Ok(BigEndian::read_i16(&bytes))
    }

    fn read_i32(&mut self) -> Result<i32, ReplicationError> {
        let bytes = self.read_bytes(4)?;
        Ok(BigEndian::read_i32(&bytes))
    }

    fn read_u32(&mut self) -> Result<u32, ReplicationError> {
        let bytes = self.read_bytes(4)?;
        Ok(BigEndian::read_u32(&bytes))
    }

    fn read_u64(&mut self) -> Result<u64, ReplicationError> {
        let bytes = self.read_bytes(8)?;
        Ok(BigEndian::read_u64(&bytes))
    }

    fn read_i64(&mut self) -> Result<i64, ReplicationError> {
        let bytes = self.read_bytes(8)?;
        Ok(BigEndian::read_i64(&bytes))
    }

    fn read_cstring(&mut self) -> Result<String, ReplicationError> {
        let start = self.offset;
        let mut idx = start;
        while idx < self.bytes.len() {
            if self.bytes[idx] == 0 {
                let out = String::from_utf8(self.bytes[start..idx].to_vec())
                    .map_err(|err| ReplicationError {
                        message: err.to_string(),
                    })?;
                self.offset = idx + 1;
                return Ok(out);
            }
            idx += 1;
        }
        Err(ReplicationError {
            message: "unterminated cstring in pgoutput message".to_string(),
        })
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, ReplicationError> {
        let end = self.offset + len;
        if end > self.bytes.len() {
            return Err(ReplicationError {
                message: "unexpected end of pgoutput message".to_string(),
            });
        }
        let out = self.bytes[self.offset..end].to_vec();
        self.offset = end;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_cstring(buf: &mut Vec<u8>, value: &str) {
        buf.extend_from_slice(value.as_bytes());
        buf.push(0);
    }

    #[test]
    fn decodes_begin_commit() {
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&1234u64.to_be_bytes());
        begin.extend_from_slice(&42i64.to_be_bytes());
        begin.extend_from_slice(&99u32.to_be_bytes());

        let mut commit = vec![b'C'];
        commit.push(0);
        commit.extend_from_slice(&200u64.to_be_bytes());
        commit.extend_from_slice(&210u64.to_be_bytes());
        commit.extend_from_slice(&55i64.to_be_bytes());

        match decode_message(&begin).unwrap() {
            PgOutputMessage::Begin(msg) => {
                assert_eq!(msg.final_lsn, 1234);
                assert_eq!(msg.commit_timestamp, 42);
                assert_eq!(msg.xid, 99);
            }
            _ => panic!("expected begin"),
        }
        match decode_message(&commit).unwrap() {
            PgOutputMessage::Commit(msg) => {
                assert_eq!(msg.flags, 0);
                assert_eq!(msg.commit_lsn, 200);
                assert_eq!(msg.end_lsn, 210);
                assert_eq!(msg.commit_timestamp, 55);
            }
            _ => panic!("expected commit"),
        }
    }

    #[test]
    fn decodes_relation_and_insert() {
        let mut relation = vec![b'R'];
        relation.extend_from_slice(&11u32.to_be_bytes());
        push_cstring(&mut relation, "public");
        push_cstring(&mut relation, "accounts");
        relation.push(1);
        relation.extend_from_slice(&2i16.to_be_bytes());
        relation.push(1);
        push_cstring(&mut relation, "id");
        relation.extend_from_slice(&20u32.to_be_bytes());
        relation.extend_from_slice(&(-1i32).to_be_bytes());
        relation.push(0);
        push_cstring(&mut relation, "name");
        relation.extend_from_slice(&25u32.to_be_bytes());
        relation.extend_from_slice(&(-1i32).to_be_bytes());

        let mut insert = vec![b'I'];
        insert.extend_from_slice(&11u32.to_be_bytes());
        insert.push(b'N');
        insert.extend_from_slice(&2i16.to_be_bytes());
        insert.push(b't');
        insert.extend_from_slice(&1i32.to_be_bytes());
        insert.push(b'1');
        insert.push(b't');
        insert.extend_from_slice(&4i32.to_be_bytes());
        insert.extend_from_slice(b"Zoe!");

        let relation_msg = decode_message(&relation).unwrap();
        match relation_msg {
            PgOutputMessage::Relation(msg) => {
                assert_eq!(msg.relation_id, 11);
                assert_eq!(msg.columns.len(), 2);
            }
            _ => panic!("expected relation"),
        }

        let insert_msg = decode_message(&insert).unwrap();
        match insert_msg {
            PgOutputMessage::Insert(msg) => {
                assert_eq!(msg.relation_id, 11);
                assert_eq!(msg.new_tuple.columns.len(), 2);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn decodes_update_delete_truncate() {
        let mut update = vec![b'U'];
        update.extend_from_slice(&7u32.to_be_bytes());
        update.push(b'K');
        update.extend_from_slice(&1i16.to_be_bytes());
        update.push(b't');
        update.extend_from_slice(&1i32.to_be_bytes());
        update.push(b'9');
        update.push(b'N');
        update.extend_from_slice(&1i16.to_be_bytes());
        update.push(b't');
        update.extend_from_slice(&1i32.to_be_bytes());
        update.push(b'8');

        let mut delete = vec![b'D'];
        delete.extend_from_slice(&7u32.to_be_bytes());
        delete.push(b'O');
        delete.extend_from_slice(&1i16.to_be_bytes());
        delete.push(b'n');

        let mut truncate = vec![b'T'];
        truncate.extend_from_slice(&2u32.to_be_bytes());
        truncate.push(0x03);
        truncate.extend_from_slice(&7u32.to_be_bytes());
        truncate.extend_from_slice(&9u32.to_be_bytes());

        assert!(matches!(decode_message(&update).unwrap(), PgOutputMessage::Update(_)));
        assert!(matches!(decode_message(&delete).unwrap(), PgOutputMessage::Delete(_)));
        match decode_message(&truncate).unwrap() {
            PgOutputMessage::Truncate(msg) => {
                assert_eq!(msg.relation_ids, vec![7, 9]);
                assert!(msg.cascade);
                assert!(msg.restart_identity);
            }
            _ => panic!("expected truncate"),
        }
    }

    #[test]
    fn decodes_origin_type_message() {
        let mut origin = vec![b'O'];
        origin.extend_from_slice(&1u64.to_be_bytes());
        push_cstring(&mut origin, "origin");

        let mut type_msg = vec![b'Y'];
        type_msg.extend_from_slice(&123u32.to_be_bytes());
        push_cstring(&mut type_msg, "public");
        push_cstring(&mut type_msg, "custom");

        let mut logical = vec![b'M'];
        logical.push(0);
        logical.extend_from_slice(&5u64.to_be_bytes());
        push_cstring(&mut logical, "prefix");
        logical.extend_from_slice(&3i32.to_be_bytes());
        logical.extend_from_slice(b"abc");

        assert!(matches!(decode_message(&origin).unwrap(), PgOutputMessage::Origin(_)));
        assert!(matches!(decode_message(&type_msg).unwrap(), PgOutputMessage::Type(_)));
        assert!(matches!(decode_message(&logical).unwrap(), PgOutputMessage::Message(_)));
    }
}
