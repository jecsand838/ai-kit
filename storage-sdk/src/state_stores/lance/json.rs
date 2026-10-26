use std::sync::{Arc, OnceLock};

use serde_json::{Map, Value};

use crate::StateStoreError;

/// Every adapter-owned reference uses atomic last-owner extraction. Public
/// values leave this domain only when an observer synchronously receives them.
#[derive(Clone)]
pub(super) struct LanceJson {
    value: Option<Arc<Value>>,
    // Cloned overlays and immutable images share one lazy encoding. The value
    // cannot change while this owner exists: public edits use Arc copy-on-write.
    encoded: Arc<OnceLock<Box<[u8]>>>,
}

impl LanceJson {
    pub(super) fn adopt(value: Arc<Value>) -> Self {
        Self {
            value: Some(value),
            encoded: Arc::new(OnceLock::new()),
        }
    }

    pub(super) fn public_value(&self) -> Arc<Value> {
        Arc::clone(self.value.as_ref().expect("owned JSON value"))
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, StateStoreError> {
        JsonDecoder {
            bytes,
            cursor: 0,
            frames: Vec::new(),
            root: None,
        }
        .decode()
    }

    pub(super) fn matches_encoding(&self, bytes: &[u8]) -> bool {
        self.encoded
            .get()
            .is_some_and(|encoded| encoded.as_ref() == bytes)
    }

    pub(super) fn encode(&self) -> Result<&[u8], StateStoreError> {
        if let Some(bytes) = self.encoded.get() {
            return Ok(bytes);
        }
        let mut bytes = Vec::new();
        let mut stack = vec![EncodeFrame::Value(
            self.value.as_ref().expect("owned JSON value"),
        )];
        while let Some(frame) = stack.pop() {
            match frame {
                EncodeFrame::Value(Value::Array(values)) => {
                    bytes.push(b'[');
                    stack.push(EncodeFrame::Array(values.iter(), true));
                }
                EncodeFrame::Value(Value::Object(values)) => {
                    bytes.push(b'{');
                    stack.push(EncodeFrame::Object(values.iter(), true));
                }
                EncodeFrame::Value(value) => {
                    serde_json::to_writer(&mut bytes, value).map_err(|_| {
                        StateStoreError::serialization("state JSON scalar could not be encoded")
                    })?;
                }
                EncodeFrame::Array(mut values, first) => {
                    if let Some(value) = values.next() {
                        if !first {
                            bytes.push(b',');
                        }
                        stack.push(EncodeFrame::Array(values, false));
                        stack.push(EncodeFrame::Value(value));
                    } else {
                        bytes.push(b']');
                    }
                }
                EncodeFrame::Object(mut values, first) => {
                    if let Some((key, value)) = values.next() {
                        if !first {
                            bytes.push(b',');
                        }
                        serde_json::to_writer(&mut bytes, key).map_err(|_| {
                            StateStoreError::serialization("state JSON key could not be encoded")
                        })?;
                        bytes.push(b':');
                        stack.push(EncodeFrame::Object(values, false));
                        stack.push(EncodeFrame::Value(value));
                    } else {
                        bytes.push(b'}');
                    }
                }
            }
        }
        // A concurrent encoder can only publish identical bytes for this
        // immutable value. Failed encodings leave the memo uninitialized.
        let _ = self.encoded.set(bytes.into_boxed_slice());
        Ok(self.encoded.get().expect("completed JSON encoding"))
    }
}

impl Drop for LanceJson {
    fn drop(&mut self) {
        if let Some(value) = self.value.take().and_then(Arc::into_inner) {
            drop(JsonTree { value });
        }
    }
}

enum EncodeFrame<'a> {
    Value(&'a Value),
    Array(std::slice::Iter<'a, Value>, bool),
    Object(serde_json::map::Iter<'a>, bool),
}

/// Protects partially decoded containers and displaced duplicate values too.
struct JsonTree {
    value: Value,
}

impl JsonTree {
    fn take(&mut self) -> Value {
        std::mem::replace(&mut self.value, Value::Null)
    }
}

impl Drop for JsonTree {
    fn drop(&mut self) {
        let mut pending = vec![self.take()];
        while let Some(value) = pending.pop() {
            match value {
                Value::Array(values) => pending.extend(values),
                Value::Object(values) => pending.extend(values.into_iter().map(|(_, value)| value)),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
            }
        }
    }
}

enum ArrayPosition {
    First,
    Value,
    Separator,
}

enum ObjectPosition {
    First,
    Key,
    Value(String),
    Separator,
}

enum DecodeFrame {
    Array {
        tree: JsonTree,
        position: ArrayPosition,
    },
    Object {
        tree: JsonTree,
        position: ObjectPosition,
    },
}

struct JsonDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
    frames: Vec<DecodeFrame>,
    root: Option<JsonTree>,
}

impl JsonDecoder<'_> {
    fn invalid() -> StateStoreError {
        StateStoreError::backend_operation("read", "stored state JSON is invalid")
    }

    fn token(&mut self) -> Option<u8> {
        while self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.cursor += 1;
        }
        self.bytes.get(self.cursor).copied()
    }

    fn decode(mut self) -> Result<LanceJson, StateStoreError> {
        self.value()?;
        while !self.frames.is_empty() {
            let token = self.token().ok_or_else(Self::invalid)?;
            match self.frames.last_mut().expect("active JSON frame") {
                DecodeFrame::Array { position, .. } => match position {
                    ArrayPosition::First if token == b']' => self.close()?,
                    ArrayPosition::First | ArrayPosition::Value => self.value()?,
                    ArrayPosition::Separator if token == b']' => self.close()?,
                    ArrayPosition::Separator if token == b',' => {
                        *position = ArrayPosition::Value;
                        self.cursor += 1;
                    }
                    ArrayPosition::Separator => return Err(Self::invalid()),
                },
                DecodeFrame::Object { position, .. } => match position {
                    ObjectPosition::First if token == b'}' => self.close()?,
                    ObjectPosition::First | ObjectPosition::Key => self.key()?,
                    ObjectPosition::Value(_) => self.value()?,
                    ObjectPosition::Separator if token == b'}' => self.close()?,
                    ObjectPosition::Separator if token == b',' => {
                        *position = ObjectPosition::Key;
                        self.cursor += 1;
                    }
                    ObjectPosition::Separator => return Err(Self::invalid()),
                },
            }
        }
        if self.token().is_some() {
            return Err(Self::invalid());
        }
        let mut root = self.root.take().ok_or_else(Self::invalid)?;
        Ok(LanceJson {
            value: Some(Arc::new(root.take())),
            // Preserve the exact valid representation, including whitespace or
            // duplicate keys. Public decoding still uses last-key-wins.
            encoded: Arc::new(OnceLock::from(self.bytes.to_vec().into_boxed_slice())),
        })
    }

    fn value(&mut self) -> Result<(), StateStoreError> {
        match self.token().ok_or_else(Self::invalid)? {
            b'[' => {
                self.cursor += 1;
                self.frames.push(DecodeFrame::Array {
                    tree: JsonTree {
                        value: Value::Array(Vec::new()),
                    },
                    position: ArrayPosition::First,
                });
            }
            b'{' => {
                self.cursor += 1;
                self.frames.push(DecodeFrame::Object {
                    tree: JsonTree {
                        value: Value::Object(Map::new()),
                    },
                    position: ObjectPosition::First,
                });
            }
            _ => {
                // Container starts are handled above. The pinned scalar parser
                // owns escaping, UTF-8, number conversion and float roundtrips.
                let mut stream = serde_json::Deserializer::from_slice(&self.bytes[self.cursor..])
                    .into_iter::<Value>();
                let value = stream
                    .next()
                    .ok_or_else(Self::invalid)?
                    .map_err(|_| Self::invalid())?;
                self.cursor += stream.byte_offset();
                self.accept(JsonTree { value })?;
            }
        }
        Ok(())
    }

    fn key(&mut self) -> Result<(), StateStoreError> {
        if self.token() != Some(b'"') {
            return Err(Self::invalid());
        }
        let mut stream =
            serde_json::Deserializer::from_slice(&self.bytes[self.cursor..]).into_iter::<String>();
        let key = stream
            .next()
            .ok_or_else(Self::invalid)?
            .map_err(|_| Self::invalid())?;
        self.cursor += stream.byte_offset();
        if self.token() != Some(b':') {
            return Err(Self::invalid());
        }
        self.cursor += 1;
        let Some(DecodeFrame::Object { position, .. }) = self.frames.last_mut() else {
            return Err(Self::invalid());
        };
        *position = ObjectPosition::Value(key);
        Ok(())
    }

    fn close(&mut self) -> Result<(), StateStoreError> {
        self.cursor += 1;
        let tree = match self.frames.pop().expect("closing JSON frame") {
            DecodeFrame::Array { tree, .. } | DecodeFrame::Object { tree, .. } => tree,
        };
        self.accept(tree)
    }

    fn accept(&mut self, mut value: JsonTree) -> Result<(), StateStoreError> {
        match self.frames.last_mut() {
            Some(DecodeFrame::Array { tree, position }) => {
                tree.value
                    .as_array_mut()
                    .expect("array frame")
                    .push(value.take());
                *position = ArrayPosition::Separator;
            }
            Some(DecodeFrame::Object { tree, position }) => {
                let ObjectPosition::Value(key) =
                    std::mem::replace(position, ObjectPosition::Separator)
                else {
                    return Err(Self::invalid());
                };
                if let Some(displaced) = tree
                    .value
                    .as_object_mut()
                    .expect("object frame")
                    .insert(key, value.take())
                {
                    drop(JsonTree { value: displaced });
                }
            }
            None => self.root = Some(value),
        }
        Ok(())
    }
}
