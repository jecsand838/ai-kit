//! Direct state serialization proof, not App persistence or executed-rurr proof.
//!
//! Expectations were derived from the checksum-verified rurr-main.zip:
//! SHA256 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e,
//! commit 005bfe3ece631e64b06dac2d42ef84235f727a4f.
//! Members: crates/rurr-core/src/state.rs:316-365,
//! state/serde_registry.rs:53-100, serde.rs:108-180, and
//! common/cow_registry.rs. App's exact public signatures belong to SPECIFICATIONS.

use std::fmt;
use std::process::{Child, Command};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use app_kit::workflow::{StateSerialization, WorkflowState};
use serde_json::{Map, Value, json};

struct CodecNames {
    prefix: String,
}

impl CodecNames {
    fn new() -> Self {
        Self {
            prefix: format!("codec_{}", uuid::Uuid::new_v4().simple()),
        }
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}_{suffix}", self.prefix)
    }

    fn state(values: impl IntoIterator<Item = (String, Value)>) -> WorkflowState {
        WorkflowState::new(Map::from_iter(values)).expect("valid test state")
    }
}

// Callback errors need only Display: neither std::error::Error, Debug, Send,
// Sync nor Clone is implemented. The closure itself remains Send + Sync.
struct DisplayOnly(Rc<str>);

impl fmt::Display for DisplayOnly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

struct CodecGate {
    armed: AtomicBool,
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl CodecGate {
    fn new() -> (Arc<Self>, mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered, observed) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                armed: AtomicBool::new(true),
                entered,
                release: Mutex::new(released),
            }),
            observed,
            release,
        )
    }

    fn pause_once(&self) -> Result<(), &'static str> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).map_err(|_| "gate observer closed")?;
            self.release
                .lock()
                .map_err(|_| "gate poisoned")?
                .recv_timeout(Duration::from_secs(3))
                .map_err(|_| "gate release timed out")?;
        }
        Ok(())
    }
}

// Reentrant callbacks can expose an incorrect lock held across caller code.
// Run that probe in one bounded, reaped child process: a broken registry must
// fail its test instead of hanging Cargo or detaching an unjoinable thread.
struct ProbeProcess {
    child: Option<Child>,
}

impl ProbeProcess {
    fn start(test: &str) -> Option<Self> {
        if std::env::var("AI_KIT_STATE_CODEC_PROBE").ok().as_deref() == Some(test) {
            return None;
        }
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", test, "--nocapture"])
            .env("AI_KIT_STATE_CODEC_PROBE", test)
            .spawn()
            .expect("start isolated codec test");
        Some(Self { child: Some(child) })
    }

    fn wait(mut self) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let child = self.child.as_mut().expect("owned probe process");
            if let Some(status) = child.try_wait().expect("observe codec probe") {
                self.child.take();
                assert!(status.success(), "isolated codec probe failed: {status}");
                return;
            }
            assert!(
                Instant::now() < deadline,
                "codec callback deadlocked or exceeded its bound"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ProbeProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _terminated = child.kill();
            let _reaped = child.wait();
        }
    }
}

struct RegisterOnDrop {
    tag: String,
    completed: Arc<AtomicUsize>,
}

impl RegisterOnDrop {
    fn value(&self) -> Value {
        json!({"old_capture": self.tag})
    }
}

impl Drop for RegisterOnDrop {
    fn drop(&mut self) {
        StateSerialization::global()
            .register_tag(self.tag.clone(), |_| {
                Ok::<_, &'static str>(json!("registered during capture destruction"))
            })
            .expect("capture destruction can reenter the global registry");
        self.completed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn ordinary_json_roundtrip_preserves_order_and_serialization_does_not_decode_tags() {
    let names = CodecNames::new();
    let unknown = names.key("unregistered");
    let tagged = json!({"__burr_serde__": unknown, "payload": [1, null, true]});
    let state = CodecNames::state([
        (
            "zeta".to_owned(),
            json!([null, true, -1, 1.5, "text", {"nested": []}]),
        ),
        ("alpha".to_owned(), tagged.clone()),
    ]);
    let encoded = state
        .serialize()
        .expect("serialization preserves tagged JSON without dispatch");
    assert_eq!(
        encoded.keys().map(String::as_str).collect::<Vec<_>>(),
        ["zeta", "alpha"]
    );
    assert_eq!(encoded["alpha"], tagged);
    assert_eq!(state.values(), &encoded);
    let plain = CodecNames::state([
        ("zeta".to_owned(), json!([null, true, -1, 1.5, "text"])),
        ("alpha".to_owned(), json!({"z": 1, "a": 2})),
    ]);
    let decoded =
        WorkflowState::deserialize(plain.serialize().expect("plain encode")).expect("plain decode");
    assert_eq!(decoded, plain);
    assert_eq!(
        decoded
            .values()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["zeta", "alpha"]
    );
    assert_eq!(
        decoded.values()["alpha"]
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
}

#[test]
fn captured_field_handlers_apply_globally_to_existing_states_and_replacements() {
    let names = CodecNames::new();
    let field = names.key("field");
    let first = CodecNames::state([(field.clone(), json!("first"))]);
    let second = CodecNames::state([(field.clone(), json!("second"))]);
    let encode_calls = Arc::new(AtomicUsize::new(0));
    let decode_calls = Arc::new(AtomicUsize::new(0));
    let encodes = Arc::clone(&encode_calls);
    let decodes = Arc::clone(&decode_calls);
    let version = names.key("version");
    let encoded_version = version.clone();
    StateSerialization::global()
        .register_field(
            field.clone(),
            move |value| {
                encodes.fetch_add(1, Ordering::SeqCst);
                Ok::<_, DisplayOnly>(json!({"version": encoded_version, "value": value}))
            },
            move |value| {
                decodes.fetch_add(1, Ordering::SeqCst);
                if value["version"] != version {
                    return Err(DisplayOnly(Rc::from("unsupported captured codec version")));
                }
                Ok(value["value"].clone())
            },
        )
        .expect("register captured field codec");
    let encoded_first = first
        .serialize()
        .expect("state created before registration sees codec");
    let encoded_second = second
        .serialize()
        .expect("another existing owner sees same global codec");
    assert_eq!(encoded_first[&field]["value"], "first");
    assert_eq!(encoded_second[&field]["value"], "second");
    assert_eq!(
        WorkflowState::deserialize(encoded_first).expect("first decode"),
        first
    );
    assert_eq!(
        WorkflowState::deserialize(encoded_second).expect("second decode"),
        second
    );
    assert_eq!(encode_calls.load(Ordering::SeqCst), 2);
    assert_eq!(decode_calls.load(Ordering::SeqCst), 2);

    StateSerialization::global()
        .register_field(
            field.clone(),
            |value| Ok::<_, &'static str>(json!({"replacement": value})),
            |value| Ok::<_, &'static str>(json!({"decoded_replacement": value["replacement"]})),
        )
        .expect("same field replacement");
    let replaced = first
        .serialize()
        .expect("existing owner observes replacement");
    assert_eq!(replaced[&field], json!({"replacement": "first"}));
    assert_eq!(
        WorkflowState::deserialize(replaced)
            .expect("replacement decoder")
            .values()[&field],
        json!({"decoded_replacement": "first"})
    );
    assert_eq!(encode_calls.load(Ordering::SeqCst), 2);
    assert_eq!(decode_calls.load(Ordering::SeqCst), 2);
    assert_eq!(first.values()[&field], "first");
}

#[test]
fn field_handlers_are_top_level_and_take_precedence_over_tag_dispatch() {
    let names = CodecNames::new();
    let field = names.key("field");
    let tag = names.key("tag");
    let tag_calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&tag_calls);
    StateSerialization::global()
        .register_tag(tag.clone(), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &'static str>(json!("must not run"))
        })
        .expect("tag registration");
    StateSerialization::global()
        .register_field(
            field.clone(),
            |value| Ok::<_, &'static str>(json!({"encoded": value})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .expect("identity field decode");
    let tagged = json!({"__burr_serde__": tag, "nested": {"__burr_serde__": names.key("unknown")}});
    let decoded = WorkflowState::deserialize(Map::from_iter([(field.clone(), tagged.clone())]))
        .expect("field decoder receives untouched input and bypasses generic dispatch of output");
    assert_eq!(decoded.values()[&field], tagged);
    assert_eq!(tag_calls.load(Ordering::SeqCst), 0);
    let nested = json!({(field.clone()): "nested field name is ordinary JSON"});
    let state = CodecNames::state([("plain".to_owned(), nested.clone())]);
    assert_eq!(
        state.serialize().expect("nested field does not encode")["plain"],
        nested
    );
    assert_eq!(
        WorkflowState::deserialize(Map::from_iter([("plain".to_owned(), nested.clone())]))
            .expect("nested field does not decode")
            .values()["plain"],
        nested
    );
}

#[test]
fn generic_decode_recurses_in_ordinary_containers_and_passes_whole_tagged_input() {
    let names = CodecNames::new();
    let tag = names.key("tag");
    let calls = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observed = Arc::clone(&calls);
    StateSerialization::global()
        .register_tag(tag.clone(), move |value| {
            observed.lock().expect("tag calls").push(value.clone());
            Ok::<_, &'static str>(value["payload"].clone())
        })
        .expect("captured tag registration");
    let first = json!({"__burr_serde__": tag, "payload": 1, "metadata": "kept for handler"});
    let second = json!({"__burr_serde__": tag, "payload": 2});
    let decoded = WorkflowState::deserialize(Map::from_iter([(
        "out".to_owned(),
        json!({"z": [first.clone(), {"a": second.clone()}], "a": false}),
    )]))
    .expect("recursive ordinary JSON decode");
    assert_eq!(
        decoded.values()["out"],
        json!({"z": [1, {"a": 2}], "a": false})
    );
    assert_eq!(*calls.lock().expect("calls"), [first, second]);
    assert_eq!(
        decoded.values()["out"]
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
}

#[test]
fn tagged_handlers_are_terminal_transformations_without_child_or_output_redispatch() {
    let names = CodecNames::new();
    let outer = names.key("outer");
    let inner = names.key("inner");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    StateSerialization::global()
        .register_tag(inner.clone(), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Err::<Value, _>("inner tag must not be inspected")
        })
        .expect("inner tag");
    StateSerialization::global()
        .register_tag(outer.clone(), |value| Ok::<_, &'static str>(value.clone()))
        .expect("identity tag handler");
    let tagged = json!({"__burr_serde__": outer, "child": {"__burr_serde__": inner}});
    let decoded = WorkflowState::deserialize(Map::from_iter([("out".to_owned(), tagged.clone())]))
        .expect("identity tag output is not a recursive dispatch cycle");
    assert_eq!(decoded.values()["out"], tagged);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn missing_and_malformed_nested_tags_preserve_pinned_error_categories() {
    let names = CodecNames::new();
    let unknown = names.key("unknown");
    let error = WorkflowState::deserialize(Map::from_iter([(
        "out".to_owned(),
        json!([{"deep": {"__burr_serde__": unknown}}]),
    )]))
    .expect_err("unregistered nested tag must fail");
    assert!(
        error
            .to_string()
            .contains(&format!("no function registered for key: {unknown}"))
    );
    for (tag, kind) in [
        (Value::Null, "null"),
        (json!(true), "bool"),
        (json!(1), "i64"),
        (json!(u64::MAX), "u64"),
        (json!(1.5), "f64"),
        (json!([]), "array"),
        (json!({}), "object"),
    ] {
        let error = WorkflowState::deserialize(Map::from_iter([(
            "out".to_owned(),
            json!({"nested": [{"__burr_serde__": tag}]}),
        )]))
        .expect_err("nonstring tag is malformed rather than an ordinary object");
        let message = error.to_string();
        assert!(message.contains("__burr_serde__"));
        assert!(message.contains("must be a string"));
        assert!(
            message.contains(kind),
            "missing pinned kind {kind}: {message}"
        );
    }
}

#[test]
fn tag_names_are_exact_data_and_replacement_is_visible_between_calls() {
    let names = CodecNames::new();
    let tag = format!("  {} / caller tag  ", names.key("tag"));
    StateSerialization::global()
        .register_tag(tag.clone(), |_| Ok::<_, &'static str>(json!(1)))
        .expect("tag is exact data, not a workflow identity");
    let encoded = Map::from_iter([("out".to_owned(), json!({"__burr_serde__": tag}))]);
    assert_eq!(
        WorkflowState::deserialize(encoded.clone())
            .expect("first handler")
            .values()["out"],
        1
    );
    StateSerialization::global()
        .register_tag(tag.clone(), |_| Ok::<_, &'static str>(json!(2)))
        .expect("replace tag");
    assert_eq!(
        WorkflowState::deserialize(encoded)
            .expect("replacement handler")
            .values()["out"],
        2
    );
    assert!(
        WorkflowState::deserialize(Map::from_iter([(
            "out".to_owned(),
            json!({"__burr_serde__": tag.trim()})
        )]))
        .is_err(),
        "tag lookup must not trim aliases"
    );
}

#[test]
fn empty_string_is_a_valid_tag_name_when_registered() {
    // The pin's StringDispatch accepts any string, including empty. Isolate
    // this one non-unique boundary key rather than polluting other tests.
    if let Some(process) = ProbeProcess::start("empty_string_is_a_valid_tag_name_when_registered") {
        process.wait();
        return;
    }
    let input = Map::from_iter([("out".to_owned(), json!({"__burr_serde__": ""}))]);
    let missing =
        WorkflowState::deserialize(input.clone()).expect_err("empty tag has no default handler");
    assert!(
        missing
            .to_string()
            .contains("no function registered for key")
    );
    StateSerialization::global()
        .register_tag(String::new(), |_| {
            Ok::<_, &'static str>(json!("empty tag decoded"))
        })
        .expect("pinned registration accepts the exact empty string");
    assert_eq!(
        WorkflowState::deserialize(input)
            .expect("empty tag lookup")
            .values()["out"],
        "empty tag decoded"
    );
}

#[test]
fn field_encoders_require_objects_but_decoders_may_return_any_json_kind() {
    let names = CodecNames::new();
    for (index, (value, kind)) in [
        (Value::Null, "null"),
        (json!(true), "bool"),
        (json!(1), "i64"),
        (json!(u64::MAX), "u64"),
        (json!(1.5), "f64"),
        (json!([]), "array"),
        (json!("text"), "string"),
    ]
    .into_iter()
    .enumerate()
    {
        let field = names.key(&format!("field_{index}"));
        let output = value.clone();
        let expected_decoded = value.clone();
        StateSerialization::global()
            .register_field(
                field.clone(),
                move |_| Ok::<_, &'static str>(output.clone()),
                move |_| Ok::<_, &'static str>(value.clone()),
            )
            .expect("field registration");
        let state = CodecNames::state([(field.clone(), json!("input"))]);
        let error = state
            .serialize()
            .expect_err("field encoder must return an object");
        let message = error.to_string();
        assert!(message.contains(&field));
        assert!(message.contains("object"));
        assert!(message.contains(kind), "missing kind {kind}: {message}");
        let decoded = WorkflowState::deserialize(Map::from_iter([(field.clone(), json!({}))]))
            .expect("the object-only rule applies to encoding, not decoding");
        assert_eq!(decoded.values()[&field], expected_decoded);
        assert_eq!(state.values()[&field], "input");
    }
}

#[test]
fn callback_errors_keep_field_direction_or_tag_context_and_display_only_cause() {
    let names = CodecNames::new();
    let field = names.key("field");
    StateSerialization::global()
        .register_field(
            field.clone(),
            |_| Err::<Value, _>(DisplayOnly(Rc::from("encoder original cause"))),
            |_| Err::<Value, _>(DisplayOnly(Rc::from("decoder original cause"))),
        )
        .expect("Display-only callback errors are supported");
    let state = CodecNames::state([(field.clone(), json!("input"))]);
    let encode = state.serialize().expect_err("encoding error").to_string();
    assert!(
        encode.contains(&field)
            && encode.contains("field serialize handler")
            && encode.contains("encoder original cause")
    );
    let decode = WorkflowState::deserialize(Map::from_iter([(field.clone(), json!({}))]))
        .expect_err("decoding error")
        .to_string();
    assert!(
        decode.contains(&field)
            && decode.contains("field deserialize handler")
            && decode.contains("decoder original cause")
    );
    let tag = names.key("tag");
    StateSerialization::global()
        .register_tag(tag.clone(), |_| {
            Err::<Value, _>(DisplayOnly(Rc::from("tag original cause")))
        })
        .expect("Display-only tag error");
    let error = WorkflowState::deserialize(Map::from_iter([(
        "out".to_owned(),
        json!({"__burr_serde__": tag}),
    )]))
    .expect_err("tag failure")
    .to_string();
    assert!(
        error.contains(&tag) && error.contains("tag original cause") && error.contains("handler")
    );
    assert_eq!(state.values()[&field], "input");
}

#[test]
fn serialization_uses_one_snapshot_while_another_thread_replaces_a_later_field() {
    let names = CodecNames::new();
    let first = names.key("a_gate");
    let later = names.key("b_later");
    let (gate, entered, release) = CodecGate::new();
    StateSerialization::global()
        .register_field(
            first.clone(),
            move |value| {
                gate.pause_once()?;
                Ok::<_, &'static str>(json!({"value": value}))
            },
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .expect("gate encoder");
    StateSerialization::global()
        .register_field(
            later.clone(),
            |_| Ok::<_, &'static str>(json!({"version": 1})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .expect("old later encoder");
    let state = CodecNames::state([(first, json!(0)), (later.clone(), json!(0))]);
    let (changed, observed_change) = mpsc::sync_channel(1);
    let (entered, changed_before_release, encoded, registered) = std::thread::scope(|scope| {
        let pending = scope.spawn(|| state.serialize());
        let entered = entered.recv_timeout(Duration::from_secs(1));
        let later = later.clone();
        let mutation = scope.spawn(move || {
            let result = StateSerialization::global().register_field(
                later,
                |_| Ok::<_, &'static str>(json!({"version": 2})),
                |value| Ok::<_, &'static str>(value.clone()),
            );
            let _observed = changed.send(());
            result
        });
        let changed_before_release = observed_change.recv_timeout(Duration::from_secs(1));
        let _released = release.send(());
        (
            entered,
            changed_before_release,
            pending.join(),
            mutation.join(),
        )
    });
    assert!(entered.is_ok(), "first handler must actually pause");
    assert!(
        changed_before_release.is_ok(),
        "registration must not wait for a callback holding a registry lock"
    );
    registered
        .expect("registry writer joined")
        .expect("replacement registered");
    let encoded = encoded.expect("encoder joined").expect("encoding succeeds");
    assert_eq!(
        encoded[&later],
        json!({"version": 1}),
        "one encode operation cannot mix snapshots"
    );
    assert_eq!(
        state.serialize().expect("next operation")[&later],
        json!({"version": 2})
    );
}

#[test]
fn deserialization_snapshots_fields_and_tags_before_calling_any_handler() {
    let names = CodecNames::new();
    let first = names.key("a_gate");
    let later = names.key("b_later");
    let tag = names.key("tag");
    let (gate, entered, release) = CodecGate::new();
    StateSerialization::global()
        .register_field(
            first.clone(),
            |value| Ok::<_, &'static str>(json!({"value": value})),
            move |value| {
                gate.pause_once()?;
                Ok::<_, &'static str>(value.clone())
            },
        )
        .expect("gate decoder");
    StateSerialization::global()
        .register_field(
            later.clone(),
            |value| Ok::<_, &'static str>(json!({"value": value})),
            |_| Ok::<_, &'static str>(json!(1)),
        )
        .expect("old field decoder");
    StateSerialization::global()
        .register_tag(tag.clone(), |_| Ok::<_, &'static str>(json!(1)))
        .expect("old tag decoder");
    let input = Map::from_iter([
        (first, json!(0)),
        (later.clone(), json!(0)),
        ("tagged".to_owned(), json!({"__burr_serde__": tag})),
    ]);
    let (changed, observed_change) = mpsc::sync_channel(1);
    let (entered, changed_before_release, decoded, registered) = std::thread::scope(|scope| {
        let pending = scope.spawn(|| WorkflowState::deserialize(input.clone()));
        let entered = entered.recv_timeout(Duration::from_secs(1));
        let later = later.clone();
        let tag = tag.clone();
        let mutation = scope.spawn(move || {
            let field = StateSerialization::global().register_field(
                later,
                |value| Ok::<_, &'static str>(json!({"value": value})),
                |_| Ok::<_, &'static str>(json!(2)),
            );
            let tag =
                StateSerialization::global().register_tag(tag, |_| Ok::<_, &'static str>(json!(2)));
            let _observed = changed.send(());
            (field, tag)
        });
        let changed_before_release = observed_change.recv_timeout(Duration::from_secs(1));
        let _released = release.send(());
        (
            entered,
            changed_before_release,
            pending.join(),
            mutation.join(),
        )
    });
    assert!(entered.is_ok(), "first decoder must pause");
    assert!(
        changed_before_release.is_ok(),
        "callbacks cannot retain registry locks"
    );
    let (field, tag_result) = registered.expect("writer joined");
    field.expect("field replaced");
    tag_result.expect("tag replaced");
    let decoded = decoded.expect("decoder joined").expect("state decoded");
    assert_eq!(decoded.values()[&later], 1);
    assert_eq!(decoded.values()["tagged"], 1);
    let next = WorkflowState::deserialize(input).expect("next decode uses new registrations");
    assert_eq!(next.values()[&later], 2);
    assert_eq!(next.values()["tagged"], 2);
}

#[test]
fn field_callbacks_can_reenter_registration_without_mutating_the_active_snapshot() {
    if let Some(process) = ProbeProcess::start(
        "field_callbacks_can_reenter_registration_without_mutating_the_active_snapshot",
    ) {
        process.wait();
        return;
    }
    let names = CodecNames::new();
    let first = names.key("a_first");
    let later = names.key("b_later");
    let tag = names.key("tag");
    let encode_later = later.clone();
    let decode_later = later.clone();
    let decode_tag = tag.clone();
    StateSerialization::global()
        .register_field(
            first.clone(),
            move |value| {
                StateSerialization::global()
                    .register_field(
                        encode_later.clone(),
                        |_| Ok::<_, &'static str>(json!({"version": 2})),
                        |_| Ok::<_, &'static str>(json!(2)),
                    )
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(json!({"value": value}))
            },
            move |value| {
                StateSerialization::global()
                    .register_field(
                        decode_later.clone(),
                        |_| Ok::<_, &'static str>(json!({"version": 3})),
                        |_| Ok::<_, &'static str>(json!(3)),
                    )
                    .map_err(|error| error.to_string())?;
                StateSerialization::global()
                    .register_tag(decode_tag.clone(), |_| Ok::<_, &'static str>(json!(3)))
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(value.clone())
            },
        )
        .expect("reentrant field handlers");
    StateSerialization::global()
        .register_field(
            later.clone(),
            |_| Ok::<_, &'static str>(json!({"version": 1})),
            |_| Ok::<_, &'static str>(json!(1)),
        )
        .expect("original later field");
    StateSerialization::global()
        .register_tag(tag.clone(), |_| Ok::<_, &'static str>(json!(1)))
        .expect("original tag");
    let state = CodecNames::state([(first.clone(), json!(0)), (later.clone(), json!(0))]);
    assert_eq!(
        state.serialize().expect("reentrant encode")[&later],
        json!({"version": 1})
    );
    assert_eq!(
        state.serialize().expect("next encode")[&later],
        json!({"version": 2})
    );
    let input = Map::from_iter([
        (first, json!(0)),
        (later.clone(), json!(0)),
        ("tagged".to_owned(), json!({"__burr_serde__": tag})),
    ]);
    let decoded = WorkflowState::deserialize(input.clone()).expect("reentrant field decode");
    assert_eq!(decoded.values()[&later], 2);
    assert_eq!(decoded.values()["tagged"], 1);
    let next = WorkflowState::deserialize(input).expect("next decode");
    assert_eq!(next.values()[&later], 3);
    assert_eq!(next.values()["tagged"], 3);
}

#[test]
fn replaced_field_and_tag_captures_drop_without_holding_the_registry_lock() {
    if let Some(process) = ProbeProcess::start(
        "replaced_field_and_tag_captures_drop_without_holding_the_registry_lock",
    ) {
        process.wait();
        return;
    }
    let names = CodecNames::new();
    let completed = Arc::new(AtomicUsize::new(0));
    let field = names.key("field");
    let tag = names.key("tag");
    let registrations = [
        names.key("encoder_drop"),
        names.key("decoder_drop"),
        names.key("tag_drop"),
    ];
    let encoder = RegisterOnDrop {
        tag: registrations[0].clone(),
        completed: Arc::clone(&completed),
    };
    let decoder = RegisterOnDrop {
        tag: registrations[1].clone(),
        completed: Arc::clone(&completed),
    };
    let tag_decoder = RegisterOnDrop {
        tag: registrations[2].clone(),
        completed: Arc::clone(&completed),
    };
    StateSerialization::global()
        .register_field(
            field.clone(),
            move |_| Ok::<_, &'static str>(encoder.value()),
            move |_| Ok::<_, &'static str>(decoder.value()),
        )
        .expect("old field captures registered");
    StateSerialization::global()
        .register_tag(tag.clone(), move |_| {
            Ok::<_, &'static str>(tag_decoder.value())
        })
        .expect("old tag capture registered");
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    StateSerialization::global()
        .register_field(
            field,
            |value| Ok::<_, &'static str>(json!({"replacement": value})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .expect("field replacement releases both captured owners outside its lock");
    assert_eq!(completed.load(Ordering::SeqCst), 2);
    StateSerialization::global()
        .register_tag(tag, |value| Ok::<_, &'static str>(value.clone()))
        .expect("tag replacement releases its captured owner outside its lock");
    assert_eq!(completed.load(Ordering::SeqCst), 3);
    for registration in registrations {
        let decoded = WorkflowState::deserialize(Map::from_iter([(
            "out".to_owned(),
            json!({"__burr_serde__": registration}),
        )]))
        .expect("reentrant registrations are globally visible");
        assert_eq!(
            decoded.values()["out"],
            "registered during capture destruction"
        );
    }
}

#[test]
fn tag_callbacks_can_replace_themselves_without_changing_in_flight_array_dispatch() {
    if let Some(process) = ProbeProcess::start(
        "tag_callbacks_can_replace_themselves_without_changing_in_flight_array_dispatch",
    ) {
        process.wait();
        return;
    }
    let names = CodecNames::new();
    let tag = names.key("self_replacing");
    let replace = tag.clone();
    StateSerialization::global()
        .register_tag(tag.clone(), move |_| {
            StateSerialization::global()
                .register_tag(replace.clone(), |_| Ok::<_, &'static str>(json!(2)))
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(json!(1))
        })
        .expect("self-replacing tag handler");
    let input = Map::from_iter([(
        "out".to_owned(),
        json!([
            {"__burr_serde__": tag}, {"__burr_serde__": tag}
        ]),
    )]);
    assert_eq!(
        WorkflowState::deserialize(input.clone())
            .expect("reentrant tags")
            .values()["out"],
        json!([1, 1])
    );
    assert_eq!(
        WorkflowState::deserialize(input)
            .expect("replacement tags")
            .values()["out"],
        json!([2, 2])
    );
}

#[test]
fn codec_outputs_and_aggregate_encoded_state_keep_existing_json_boundaries() {
    // Existing App JSON byte/depth limits apply after custom transformations,
    // too. These are App bounds, not limits claimed for the pinned rurr core.
    let names = CodecNames::new();
    let field = names.key("large");
    StateSerialization::global()
        .register_field(
            field.clone(),
            |_| Ok::<_, &'static str>(json!({"large": "x".repeat(2_097_152)})),
            |_| Ok::<_, &'static str>(json!("x".repeat(2_097_152))),
        )
        .expect("large-output codec");
    let state = CodecNames::state([(field.clone(), json!(0))]);
    assert!(
        state.serialize().is_err(),
        "encoded codec output must retain the byte bound"
    );
    assert!(
        WorkflowState::deserialize(Map::from_iter([(field.clone(), json!(0))])).is_err(),
        "decoded codec output must retain the byte bound"
    );
    assert_eq!(state.values()[&field], 0);

    let mut fields = Vec::new();
    for index in 0..2 {
        let field = names.key(&format!("aggregate_{index}"));
        StateSerialization::global()
            .register_field(
                field.clone(),
                |_| Ok::<_, &'static str>(json!({"large": "x".repeat(600_000)})),
                |value| Ok::<_, &'static str>(value.clone()),
            )
            .expect("aggregate codec");
        fields.push((field, json!(0)));
    }
    assert!(
        CodecNames::state(fields).serialize().is_err(),
        "valid individual outputs cannot bypass the complete encoded-state byte bound"
    );

    let deep = names.key("deep");
    StateSerialization::global()
        .register_field(
            deep.clone(),
            |_| {
                let mut value = json!(0);
                for _ in 0..128 {
                    value = json!([value]);
                }
                Ok::<_, &'static str>(json!({"nested": value}))
            },
            |_| {
                let mut value = json!(0);
                for _ in 0..128 {
                    value = json!([value]);
                }
                Ok::<_, &'static str>(value)
            },
        )
        .expect("nested-output codec");
    assert!(
        CodecNames::state([(deep.clone(), json!(0))])
            .serialize()
            .is_err(),
        "encoded depth is bounded after field transformation"
    );
    assert!(
        WorkflowState::deserialize(Map::from_iter([(deep, json!(0))])).is_err(),
        "decoded depth is bounded after field transformation"
    );
}
