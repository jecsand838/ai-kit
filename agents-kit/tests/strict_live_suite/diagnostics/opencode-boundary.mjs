import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, openSync, closeSync, lstatSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// Test-only native plugin. No raw request, response, identifier, header, URL,
// description, prompt, or argument value enters a receipt. The cloud case also
// stages bounded private bodies separately; its caller owns their disposition.
class BoundaryCapture {
  static REQUEST_LIMIT = 2 << 20;
  static FRAME_LIMIT = 256 << 10;
  static RESPONSE_LIMIT = 8 << 20;
  static ARGUMENT_LIMIT = 64 << 10;
  static FLOW_LIMIT = 16;
  static RECEIPT_LIMIT = 128 << 10;
  static FRESH_TEXT_LIMIT = 64 << 10;
  static FRESH_PART_LIMIT = 16;

  constructor(options) {
    this.options = options;
    this.provider = options.expectedProvider ?? "ollama";
    this.flow = 0;
    this.written = 0;
    this.receiptPersistenceComplete = true;
    this.capture = randomUUID();
    this.path = join(options.outputDirectory, `opencode-boundary-${process.pid}-${this.capture}.jsonl`);
    const fd = openSync(this.path, "wx", 0o600);
    closeSync(fd);
    if (options.freshOutput !== undefined) {
      const expectation = options.freshOutput;
      this.fresh = { root: undefined, assistants: new Map(), pendingText: new Map(), excludedAssistants: new Set(),
        nativeBytes: 0, flows: new Map(), incomplete: false };
      if (typeof expectation?.prompt !== "string" || !expectation.prompt.length
        || Buffer.byteLength(expectation.prompt) > 4096
        || typeof expectation?.expected !== "string" || !expectation.expected.length
        || Buffer.byteLength(expectation.expected) > 256) {
        this.freshIncomplete("expectation_shape");
      } else {
        this.fresh.prompt = expectation.prompt;
        this.fresh.expected = expectation.expected;
      }
    }
  }

  emit(facts) {
    try {
      if (this.receiptsExhausted) return;
      let line = JSON.stringify({ version: 1, capture_id: this.capture, ...facts }) + "\n";
      if (Buffer.byteLength(line) > 8192) {
        this.receiptIncomplete();
        line = JSON.stringify({ version: 1, capture_id: this.capture, event: "observer_incomplete", flow: facts.flow, reason: "receipt_record_bound" }) + "\n";
      }
      if (this.written + Buffer.byteLength(line) > BoundaryCapture.RECEIPT_LIMIT - 256) {
        this.receiptsExhausted = true;
        this.receiptIncomplete();
        line = JSON.stringify({ version: 1, capture_id: this.capture, event: "observer_incomplete", reason: "receipt_total_bound" }) + "\n";
      }
      appendFileSync(this.path, line, { encoding: "utf8", mode: 0o600 });
      this.written += Buffer.byteLength(line);
    } catch {
      this.receiptIncomplete();
      // Diagnostics never replace a native request, stream, or cleanup outcome.
    }
  }

  receiptIncomplete() {
    this.receiptPersistenceComplete = false;
    if (this.fresh) this.fresh.incomplete = true;
  }

  static fingerprint(value) {
    return createHash("sha256").update(value).digest("hex");
  }

  static shape(value) {
    if (value === null) return "null";
    if (Array.isArray(value)) return "array";
    return typeof value;
  }

  static identity(value) {
    return typeof value === "string" && value.length > 0 && value.length <= 256;
  }

  freshIncomplete(reason) {
    if (!this.fresh) return;
    this.fresh.incomplete = true;
    this.emit({ event: "fresh_incomplete", capture: "incomplete", reason, foreground_http_body_observed: false });
  }

  chatMessage(input, output) {
    if (!this.fresh || this.fresh.incomplete) return;
    try {
      if (!Array.isArray(output?.parts) || output.parts.length > BoundaryCapture.FRESH_PART_LIMIT) {
        this.freshIncomplete("user_parts_shape");
        return;
      }
      const matching = output.parts.filter((part) => part?.type === "text"
        && part.synthetic !== true && part.text === this.fresh.prompt);
      if (!matching.length) return;
      const message = output.message;
      const part = matching[0];
      if (matching.length !== 1 || message?.role !== "user"
        || !BoundaryCapture.identity(input?.sessionID) || !BoundaryCapture.identity(message.id)
        || message.sessionID !== input.sessionID || part.sessionID !== input.sessionID
        || part.messageID !== message.id || !BoundaryCapture.identity(part.id)
        || (input.messageID !== undefined && input.messageID !== message.id)
        || input.model?.providerID !== this.provider || input.model?.modelID !== this.expectedNativeModel
        || message.model?.providerID !== this.provider || message.model?.modelID !== this.expectedNativeModel) {
        this.freshIncomplete("user_ownership");
        return;
      }
      if (this.fresh.root) {
        this.freshIncomplete("duplicate_fresh_root");
        return;
      }
      this.fresh.root = { session: input.sessionID, user: message.id };
      this.emit({ event: "fresh_user", capture: "complete", prompt_matches_expected: true,
        ...(this.provider === "openai" ? {
          native_session_sha256: BoundaryCapture.fingerprint(input.sessionID),
          native_user_sha256: BoundaryCapture.fingerprint(message.id),
        } : {}),
        agent_matches_build: message.agent === "build" });
    } catch {
      this.freshIncomplete("user_observer_error");
    }
  }

  nativeEvent(input) {
    if (!this.fresh?.root || this.fresh.incomplete) return;
    try {
      if (input?.event?.type !== "message.updated") return;
      const info = input.event.properties?.info;
      if (this.fresh.assistants.has(info?.id)
        && (info.role !== "assistant" || info.sessionID !== this.fresh.root.session || info.parentID !== this.fresh.root.user)) {
        this.freshIncomplete("assistant_owner_changed");
        return;
      }
      if (info?.role !== "assistant" || info.sessionID !== this.fresh.root.session) return;
      if (info.parentID !== this.fresh.root.user) {
        if (BoundaryCapture.identity(info.id)) {
          if (this.fresh.excludedAssistants.size >= BoundaryCapture.FRESH_PART_LIMIT) {
            this.freshIncomplete("unrelated_assistant_bound");
            return;
          }
          this.fresh.excludedAssistants.add(info.id);
          this.fresh.pendingText.delete(info.id);
          this.freshSummary(false);
        }
        return;
      }
      if (!BoundaryCapture.identity(info.id) || info.providerID !== this.provider || info.modelID !== this.expectedNativeModel) {
        this.freshIncomplete("assistant_identity_or_model");
        return;
      }
      let assistant = this.fresh.assistants.get(info.id);
      if (!assistant) {
        if (this.fresh.excludedAssistants.has(info.id)) {
          this.freshIncomplete("assistant_owner_changed");
          return;
        }
        if (this.fresh.assistants.size) {
          this.freshIncomplete("ambiguous_native_assistants");
          return;
        }
        assistant = this.fresh.pendingText.get(info.id) ?? { parts: new Map(), terminal: false };
        this.fresh.pendingText.delete(info.id);
        this.fresh.assistants.set(info.id, assistant);
      }
      if (info.time?.completed !== undefined) {
        if (!Number.isFinite(info.time.completed) || info.finish !== "stop" || Object.hasOwn(info, "error")) {
          this.freshIncomplete("native_terminal_shape");
          return;
        }
        assistant.terminal = true;
        this.freshSummary(false);
      }
    } catch {
      this.freshIncomplete("event_observer_error");
    }
  }

  nativeText(input, output) {
    if (!this.fresh?.root || this.fresh.incomplete) return;
    try {
      if (input?.sessionID !== this.fresh.root.session) return;
      if (this.fresh.excludedAssistants.has(input.messageID)) return;
      let assistant = this.fresh.assistants.get(input.messageID) ?? this.fresh.pendingText.get(input.messageID);
      if (!assistant) {
        if (!BoundaryCapture.identity(input.messageID) || this.fresh.pendingText.size >= BoundaryCapture.FRESH_PART_LIMIT) {
          this.freshIncomplete("unknown_text_owner_bound");
          return;
        }
        assistant = { parts: new Map(), terminal: false };
        this.fresh.pendingText.set(input.messageID, assistant);
      }
      if (!BoundaryCapture.identity(input.partID) || typeof output?.text !== "string"
        || assistant.parts.has(input.partID)) {
        this.freshIncomplete("native_text_ownership");
        return;
      }
      this.fresh.nativeBytes += Buffer.byteLength(output.text);
      if (assistant.parts.size >= BoundaryCapture.FRESH_PART_LIMIT || this.fresh.nativeBytes > BoundaryCapture.FRESH_TEXT_LIMIT) {
        this.freshIncomplete("native_text_bound");
        return;
      }
      assistant.parts.set(input.partID, output.text);
      this.freshSummary(false);
    } catch {
      this.freshIncomplete("text_observer_error");
    }
  }

  freshRequest(request, init, flow) {
    if (!this.fresh || this.fresh.incomplete) return false;
    const messages = this.provider === "openai" ? request.input : request.messages;
    if (!Array.isArray(messages) || messages.length > 256) {
      this.freshIncomplete("request_messages_shape");
      return false;
    }
    const user = messages.findLast((message) => message?.role === "user");
    if (!user) return false;
    let texts;
    if (typeof user.content === "string") texts = [user.content];
    else if (Array.isArray(user.content) && user.content.length <= BoundaryCapture.FRESH_PART_LIMIT
      && user.content.every((part) => part?.type === (this.provider === "openai" ? "input_text" : "text") && typeof part.text === "string")) {
      texts = user.content.map((part) => part.text);
    } else {
      this.freshIncomplete("request_user_content_shape");
      return false;
    }
    const matches = texts.filter((text) => text === this.fresh.prompt).length;
    if (!matches) return false;
    const modelMatches = request.model === this.expectedModel;
    const sessionMatches = this.fresh.root !== undefined
      && new Headers(init?.headers).get("x-opencode-session-id") === this.fresh.root.session;
    const temperaturePresent = Object.hasOwn(request, "temperature");
    const temperatureNumeric = typeof request.temperature === "number" && Number.isFinite(request.temperature);
    // Observe the serialized request only. Settings are scenario inputs fixed
    // before startup; this observer never assigns or repairs request parameters.
    const temperatureExpected = Object.hasOwn(this.options, "expectedTemperature");
    this.emit({ event: "fresh_request", flow, capture: matches === 1 && modelMatches && sessionMatches ? "complete" : "incomplete",
      model_matches_configured: modelMatches, session_matches_fresh_user: sessionMatches, exact_prompt_parts: matches,
      ...(this.provider === "openai" ? { user_text_parts: texts.length, other_user_text_parts: texts.length - matches } : {}),
      temperature_present: temperaturePresent, temperature_numeric: temperatureNumeric,
      temperature_value: temperatureNumeric ? request.temperature : null,
      temperature_matches_scenario: temperatureExpected ? temperatureNumeric && request.temperature === this.options.expectedTemperature : null });
    if (matches !== 1 || !modelMatches || !sessionMatches
      || (this.provider === "openai" && request.stream !== true)) {
      this.freshIncomplete("request_correlation");
      return false;
    }
    if (this.fresh.flows.size) {
      this.freshIncomplete("ambiguous_provider_flows");
      return false;
    }
    this.fresh.flows.set(flow, { complete: false, text: "" });
    return true;
  }

  freshResponse(flow, complete, text, reason, snapshots) {
    const response = this.fresh?.flows.get(flow);
    if (!response) return;
    response.complete = complete;
    response.text = complete ? text : "";
    if (snapshots !== undefined) {
      response.finalText = complete ? snapshots.final : "";
      response.providerFacts = {
        provider_delta_complete: complete,
        provider_done_complete: complete,
        provider_final_complete: complete,
        ...(complete ? {
          provider_delta_bytes: Buffer.byteLength(text),
          provider_done_bytes: Buffer.byteLength(snapshots.done),
          provider_final_bytes: Buffer.byteLength(snapshots.final),
          provider_delta_sha256: BoundaryCapture.fingerprint(text),
          provider_done_sha256: BoundaryCapture.fingerprint(snapshots.done),
          provider_final_sha256: BoundaryCapture.fingerprint(snapshots.final),
          provider_delta_equals_expected: text === this.fresh.expected,
          provider_done_equals_expected: snapshots.done === this.fresh.expected,
          provider_final_equals_expected: snapshots.final === this.fresh.expected,
          provider_delta_equals_done: text === snapshots.done,
          provider_delta_equals_final: text === snapshots.final,
          provider_done_equals_final: snapshots.done === snapshots.final,
          provider_content_part_equals_done: snapshots.part === snapshots.done,
          provider_item_equals_done: snapshots.item === snapshots.done,
          provider_text_parts: snapshots.parts.length,
          provider_parts_delta_equal_done: snapshots.parts.every((part) => part.delta === part.doneText),
          provider_parts_done_equal_content: snapshots.parts.every((part) => part.doneText === part.partText),
          provider_parts_content_equal_item: snapshots.parts.every((part) => part.partText === part.itemText),
          provider_parts_item_equal_final: snapshots.parts.every((part) => part.itemText === part.finalText),
          provider_reported_model_matches_requested: snapshots.model === this.expectedModel,
        } : {}),
      };
      if (complete) {
        for (const part of snapshots.parts) {
          this.emit({ event: "fresh_provider_part", flow, capture: "complete",
            output_index: part.output_index, content_index: part.content_index,
            delta_bytes: Buffer.byteLength(part.delta), done_bytes: Buffer.byteLength(part.doneText),
            content_bytes: Buffer.byteLength(part.partText), item_bytes: Buffer.byteLength(part.itemText), final_bytes: Buffer.byteLength(part.finalText),
            delta_sha256: BoundaryCapture.fingerprint(part.delta), done_sha256: BoundaryCapture.fingerprint(part.doneText),
            content_sha256: BoundaryCapture.fingerprint(part.partText), item_sha256: BoundaryCapture.fingerprint(part.itemText), final_sha256: BoundaryCapture.fingerprint(part.finalText),
            delta_equals_done: part.delta === part.doneText, done_equals_content: part.doneText === part.partText,
            content_equals_item: part.partText === part.itemText, item_equals_final: part.itemText === part.finalText,
          });
        }
      }
    }
    this.emit({ event: "fresh_provider_text", flow, capture: complete ? "complete" : "incomplete",
      ...(complete ? { text_bytes: Buffer.byteLength(text), text_equals_expected: text === this.fresh.expected,
        text_equals_asterisk_wrapped_expected: text === `*${this.fresh.expected}*` } : { reason }),
      ...response.providerFacts,
      foreground_http_body_observed: false });
    if (!complete) this.freshIncomplete("provider_text_incomplete");
    this.freshSummary(false);
  }

  freshSummary(final) {
    if (!this.fresh) return;
    const flows = [...this.fresh.flows.values()];
    const assistants = [...this.fresh.assistants.values()];
    const complete = !this.fresh.incomplete && this.fresh.root !== undefined
      && flows.length === 1 && flows[0].complete && assistants.length === 1
      && assistants[0].terminal && assistants[0].parts.size > 0 && this.fresh.pendingText.size === 0;
    if (!complete && !final) return;
    const native = complete ? [...assistants[0].parts].sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
      .map(([, text]) => text).join("") : undefined;
    this.emit({ event: final ? "fresh_summary" : "fresh_comparison", capture: complete ? "complete" : "incomplete",
      matching_provider_flows: flows.length, matching_assistants: assistants.length,
      receipt_persistence_complete: this.receiptPersistenceComplete,
      foreground_http_body_observed: false,
      ...(complete ? {
        provider_equals_expected: flows[0].text === this.fresh.expected,
        provider_equals_asterisk_wrapped_expected: flows[0].text === `*${this.fresh.expected}*`,
        native_equals_expected: native === this.fresh.expected, native_equals_provider: native === flows[0].text,
        native_text_parts: assistants[0].parts.size, native_text_bytes: Buffer.byteLength(native),
        ...(this.provider === "openai" ? {
          native_text_sha256: BoundaryCapture.fingerprint(native),
          native_session_sha256: BoundaryCapture.fingerprint(this.fresh.root.session),
          native_user_sha256: BoundaryCapture.fingerprint(this.fresh.root.user),
          native_assistant_sha256: BoundaryCapture.fingerprint(this.fresh.assistants.keys().next().value),
          native_parts: [...assistants[0].parts].map(([id, text]) => ({
            part_sha256: BoundaryCapture.fingerprint(id), text_sha256: BoundaryCapture.fingerprint(text), text_bytes: Buffer.byteLength(text),
          })),
        } : {}),
        ...flows[0].providerFacts,
        ...(flows[0].providerFacts ? {
          native_equals_provider_delta: native === flows[0].text,
          native_equals_provider_final: native === flows[0].finalText,
          native_text_is_hook_checkpoint: true,
        } : {}),
      } : {}),
    });
    if (final) {
      this.fresh.assistants.clear();
      this.fresh.pendingText.clear();
      this.fresh.excludedAssistants.clear();
      this.fresh.flows.clear();
      this.fresh.prompt = undefined;
      this.fresh.expected = undefined;
      this.fresh.root = undefined;
    }
  }

  static schema(value, depth = 0) {
    if (depth > 5) return { bounded: true };
    if (typeof value === "boolean") return { boolean: value };
    if (!value || typeof value !== "object" || Array.isArray(value)) return { shape: this.shape(value) };
    const types = new Set(["object", "array", "string", "boolean", "number", "integer", "null"]);
    const names = new Set(["questions", "question", "header", "options", "label", "description", "multiple", "custom"]);
    const result = { type: types.has(value.type) ? value.type : this.shape(value.type) };
    if (Array.isArray(value.required)) {
      result.required = [...names].filter((name) => value.required.includes(name));
      result.other_required_count = value.required.length - result.required.length;
    }
    if (value.properties && typeof value.properties === "object") {
      result.properties = {};
      for (const name of names) {
        if (Object.hasOwn(value.properties, name)) result.properties[name] = this.schema(value.properties[name], depth + 1);
      }
      result.other_property_count = Object.keys(value.properties).filter((name) => !names.has(name)).length;
    }
    if (Object.hasOwn(value, "items")) result.items = this.schema(value.items, depth + 1);
    if (Object.hasOwn(value, "$ref")) result.reference = this.shape(value.$ref);
    if (Object.hasOwn(value, "additionalProperties")) {
      result.additional_properties = typeof value.additionalProperties === "boolean" ? value.additionalProperties : this.shape(value.additionalProperties);
    }
    for (const key of ["anyOf", "oneOf", "allOf"]) {
      if (Array.isArray(value[key])) result[key] = value[key].slice(0, 4).map((item) => this.schema(item, depth + 1));
    }
    return result;
  }

  install(config) {
    const provider = config.provider?.[this.provider];
    const supported = this.provider === "ollama"
      ? provider?.npm === "@ai-sdk/openai-compatible"
      : this.provider === "openai" && (provider?.npm === undefined || provider.npm === "@ai-sdk/openai");
    if (!supported || !provider?.options) {
      this.emit({ event: "unavailable", reason: "provider_shape" });
      return;
    }
    const expected = Object.entries(provider.models ?? {}).filter(([, model]) => model.name === this.options.expectedModelName);
    if (expected.length !== 1) {
      this.emit({ event: "unavailable", reason: "configured_model_ambiguity" });
      return;
    }
    this.expectedModel = expected[0][1].id ?? expected[0][0];
    this.expectedNativeModel = expected[0][0];
    const delegate = provider.options.fetch ?? globalThis.fetch;
    if (typeof delegate !== "function") {
      this.emit({ event: "unavailable", reason: "fetch_shape" });
      return;
    }
    const owner = this;
    provider.options.fetch = function (...args) {
      return owner.fetch(delegate, this, args);
    };
    this.emit({ event: "installed", boundary: "serialized_provider_http", expected_role: this.provider === "openai" ? "opencode_cloud" : this.options.expectedRole === "opencode_default" ? "opencode_default" : "opencode_shell", flow_limit: BoundaryCapture.FLOW_LIMIT });
  }

  async fetch(delegate, receiver, args) {
    let observation;
    if (this.flow < BoundaryCapture.FLOW_LIMIT) {
      const flow = ++this.flow;
      try {
        observation = new BoundaryResponse(this, flow);
        const body = args[1]?.body;
        if (typeof body !== "string") {
          this.emit({ event: "request", flow, capture: "incomplete", reason: "non_string_body" });
          this.freshIncomplete("request_body_shape");
        } else if (Buffer.byteLength(body) > BoundaryCapture.REQUEST_LIMIT) {
          this.emit({ event: "request", flow, capture: "incomplete", reason: "request_bound" });
          this.freshIncomplete("request_bound");
        } else {
          const request = JSON.parse(body);
          observation.fresh = this.freshRequest(request, args[1], flow);
          if (observation.fresh && this.provider === "openai") observation.stageRequest(body);
          const tools = Array.isArray(request.tools) ? request.tools : [];
          const questions = tools.filter((tool) => tool?.type === "function"
            && (this.provider === "openai" ? tool.name : tool.function?.name) === "question");
          const facts = {
            event: "request", flow, capture: "complete",
            model_matches_configured: request.model === this.expectedModel,
            model_shape: BoundaryCapture.shape(request.model),
            streamed: request.stream === true,
            tools_shape: BoundaryCapture.shape(request.tools), tool_count: tools.length,
            question_definition_count: questions.length,
            tool_choice: ["auto", "none", "required"].includes(request.tool_choice) ? request.tool_choice : BoundaryCapture.shape(request.tool_choice),
          };
          if (questions.length === 1) {
            const schema = this.provider === "openai" ? questions[0].parameters : questions[0].function.parameters;
            facts.question_schema = BoundaryCapture.schema(schema);
            facts.question_schema_sha256 = createHash("sha256").update(JSON.stringify(schema) ?? "undefined").digest("hex");
          }
          this.emit(facts);
        }
      } catch {
        this.emit({ event: "request", flow, capture: "incomplete", reason: "request_json" });
        this.freshIncomplete("request_observer_error");
      }
    } else if (!this.flowsExhausted) {
      this.flowsExhausted = true;
      this.emit({ event: "observer_incomplete", reason: "flow_bound" });
      this.freshIncomplete("flow_bound");
    }
    let response;
    try {
      response = await Reflect.apply(delegate, receiver, args);
    } catch (error) {
      observation?.end("fetch_error");
      throw error;
    }
    if (!observation) return response;
    try {
      return observation.observe(response);
    } catch {
      observation.end("observer_setup_error");
      return response;
    }
  }
}

class BoundaryResponse {
  constructor(owner, flow) {
    this.owner = owner;
    this.flow = flow;
    this.bytes = 0;
    this.textBytes = 0;
    this.frames = 0;
    this.invalidFrames = 0;
    this.pending = "";
    this.data = [];
    this.dataBytes = 0;
    this.calls = new Map();
    this.finish = new Set();
    this.decoder = new TextDecoder();
    this.done = false;
    this.complete = true;
    this.fresh = false;
    this.freshText = "";
    this.freshTextBytes = 0;
    this.freshTextComplete = true;
    this.freshTerminalSeen = false;
    this.freshDone = false;
    if (owner.provider === "openai") {
      this.responses = { phase: "awaiting", sequence: -1, items: new Map(), bytes: 0 };
    }
  }

  stageRequest(body) {
    const stem = `${this.owner.capture}-flow-${this.flow}`;
    this.privateCapture = { directory: join(this.owner.options.outputDirectory, "private-pending"),
      request: `${stem}.request.json`, response: `${stem}.response.body`, manifest: `${stem}.manifest.json`,
      requestBytes: Buffer.byteLength(body), responseBytes: 0, capturedBytes: 0, bounded: false, failed: false, directoryReady: false };
    const capture = this.privateCapture;
    try {
      capture.bytes = Buffer.alloc(BoundaryCapture.RESPONSE_LIMIT);
      try { mkdirSync(capture.directory, { mode: 0o700 }); } catch (error) {
        if (error.code !== "EEXIST") throw error;
      }
      const directory = lstatSync(capture.directory);
      if (!directory.isDirectory() || (directory.mode & 0o777) !== 0o700) throw new Error("private_directory_mode");
      capture.directoryReady = true;
      writeFileSync(join(capture.directory, capture.request), body, { flag: "wx", mode: 0o600 });
    } catch {
      capture.failed = true;
      this.owner.freshIncomplete("private_request_write");
    }
  }

  stageChunk(value) {
    const capture = this.privateCapture;
    if (!capture) return;
    try {
      capture.responseBytes += value.byteLength;
      const remaining = BoundaryCapture.RESPONSE_LIMIT - capture.capturedBytes;
      if (remaining > 0 && capture.bytes) {
        const bytes = value.subarray(0, remaining);
        capture.bytes.set(bytes, capture.capturedBytes);
        capture.capturedBytes += bytes.byteLength;
      }
      if (capture.responseBytes > BoundaryCapture.RESPONSE_LIMIT) capture.bounded = true;
    } catch {
      capture.failed = true;
      this.owner.freshIncomplete("private_response_capture");
    }
  }

  stageEnd(outcome) {
    const capture = this.privateCapture;
    if (!capture) return;
    if (!capture.directoryReady) {
      capture.bytes = undefined;
      return;
    }
    try {
      writeFileSync(join(capture.directory, capture.response), capture.bytes.subarray(0, capture.capturedBytes), { flag: "wx", mode: 0o600 });
      writeFileSync(join(capture.directory, capture.manifest), JSON.stringify({
        version: 1, protocol: "openai_responses", capture_id: this.owner.capture, flow: this.flow,
        request_file: capture.request, response_file: capture.response,
        request_bytes: capture.requestBytes, response_bytes: capture.responseBytes, captured_response_bytes: capture.capturedBytes,
        outcome, status: this.status ?? null, eof: outcome === "eof", response_bound_exceeded: capture.bounded,
        body_capture_complete: !capture.failed && !capture.bounded && outcome === "eof",
        provider_correlation_complete: this.complete && this.invalidFrames === 0 && this.responses.phase === "completed",
        receipt_persistence_complete: this.owner.receiptPersistenceComplete,
      }) + "\n", { flag: "wx", mode: 0o600 });
      this.owner.emit({ event: "private_bodies_staged", flow: this.flow,
        capture: !capture.failed && !capture.bounded && outcome === "eof" ? "complete" : "incomplete",
        request_bytes: capture.requestBytes, response_bytes: capture.responseBytes, captured_response_bytes: capture.capturedBytes,
      });
    } catch {
      capture.failed = true;
      this.owner.freshIncomplete("private_response_write");
    } finally {
      capture.bytes = undefined;
    }
  }

  observe(response) {
    this.status = response.status;
    if (this.fresh) this.decoder = new TextDecoder("utf-8", { fatal: true });
    this.format = response.headers.get("content-type")?.includes("text/event-stream") ? "sse" : "json";
    this.owner.emit({ event: "response_headers", flow: this.flow, status: response.status, format: this.format });
    if (!response.body) {
      this.end("no_body");
      return response;
    }
    if (response.body.locked) {
      this.end("body_already_locked");
      return response;
    }
    let reader;
    const observer = this;
    const body = new ReadableStream({
      async pull(controller) {
        try {
          const item = await reader.read();
          if (item.done) {
            observer.end("eof");
            reader.releaseLock();
            controller.close();
          } else {
            observer.chunk(item.value);
            controller.enqueue(item.value);
          }
        } catch (error) {
          observer.end("read_error");
          reader.releaseLock();
          controller.error(error);
        }
      },
      async cancel(reason) {
        observer.end("cancelled");
        try {
          await reader.cancel(reason);
        } finally {
          reader.releaseLock();
        }
      },
    }, { highWaterMark: 0 });
    const observed = new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
    for (const key of ["url", "redirected", "type"]) {
      Object.defineProperty(observed, key, { configurable: true, get: () => response[key] });
    }
    reader = response.body.getReader();
    return observed;
  }

  chunk(value) {
    this.stageChunk(value);
    try {
      if (!this.complete) return;
      this.bytes += value.byteLength;
      if (this.bytes > BoundaryCapture.RESPONSE_LIMIT) return this.bound("response_bound");
      this.pending += this.decoder.decode(value, { stream: true });
      if (this.format === "sse") {
        let newline;
        while ((newline = this.pending.indexOf("\n")) >= 0) {
          const line = this.pending.slice(0, newline).replace(/\r$/, "");
          this.pending = this.pending.slice(newline + 1);
          if (line === "") {
            if (this.data.length) this.frame(this.data.join("\n"));
            this.data = [];
            this.dataBytes = 0;
          } else if (line.startsWith("data:")) {
            if (line.length > BoundaryCapture.FRAME_LIMIT) return this.bound("frame_bound");
            this.data.push(line.slice(5).replace(/^ /, ""));
            this.dataBytes += line.length;
          }
          if (this.data.length > 256 || this.dataBytes > BoundaryCapture.FRAME_LIMIT) return this.bound("frame_bound");
        }
      }
      if (this.pending.length > BoundaryCapture.FRAME_LIMIT) this.bound("frame_bound");
    } catch {
      this.bound("observer_decode_error");
    }
  }

  bound(reason) {
    this.complete = false;
    this.freshTextComplete = false;
    this.freshText = "";
    this.pending = "";
    this.data = [];
    this.dataBytes = 0;
    this.calls.clear();
    this.responses?.items.clear();
    this.owner.emit({ event: "response_incomplete", flow: this.flow, reason });
  }

  responsesRequire(condition, reason) {
    if (!condition) throw new Error(reason);
  }

  responsesIndex(value) {
    return Number.isSafeInteger(value) && value >= 0 && value < BoundaryCapture.FRESH_PART_LIMIT;
  }

  responsesText(value) {
    this.responsesRequire(typeof value === "string", "responses_text_shape");
    this.responses.bytes += Buffer.byteLength(value);
    this.responsesRequire(this.responses.bytes <= BoundaryCapture.FRESH_TEXT_LIMIT, "responses_text_bound");
    return value;
  }

  responsesItem(frame, kind) {
    const item = this.responses.items.get(frame.output_index);
    this.responsesRequire(this.responsesIndex(frame.output_index) && item !== undefined && item.id === frame.item_id
      && item.kind === kind && !item.done, "responses_item_correlation");
    return item;
  }

  responsesContent(frame) {
    const item = this.responsesItem(frame, "message");
    const part = item.parts.get(frame.content_index);
    this.responsesRequire(this.responsesIndex(frame.content_index) && part !== undefined,
      "responses_content_correlation");
    return part;
  }

  responsesReasoning(item) {
    this.responsesRequire((item.status === undefined || item.status === "completed")
      && Array.isArray(item.summary) && item.summary.length <= BoundaryCapture.FRESH_PART_LIMIT
      && item.summary.every((part) => part?.type === "summary_text" && typeof part.text === "string"),
    "responses_reasoning_shape");
  }

  responsesMessage(item, observed, field) {
    this.responsesRequire(item.role === "assistant" && item.status === "completed"
      && Array.isArray(item.content) && item.content.length === observed.parts.size
      && item.content.length > 0, "responses_message_shape");
    for (const [index, value] of item.content.entries()) {
      const part = observed.parts.get(index);
      this.responsesRequire(value?.type === "output_text" && part?.phase === "part_done",
        "responses_message_content");
      part[field] = this.responsesText(value.text);
    }
  }

  responsesSnapshot(response, status) {
    const state = this.responses;
    this.responsesRequire(response && typeof response === "object" && !Array.isArray(response)
      && response.id === state.id && response.model === state.model && response.status === status
      && response.error == null && response.incomplete_details == null && Array.isArray(response.output),
    "responses_snapshot_correlation");
    return response;
  }

  // Responses text is observed at each boundary independently. Disagreement
  // between a delta and a terminal snapshot is evidence, never a repair rule.
  responsesFrame(frame) {
    try {
      const state = this.responses;
      this.responsesRequire(frame && typeof frame === "object" && !Array.isArray(frame)
        && Number.isSafeInteger(frame.sequence_number) && frame.sequence_number === state.sequence + 1
        && state.phase !== "completed", "responses_event_sequence");
      state.sequence = frame.sequence_number;
      if (frame.type === "response.created") {
        const response = frame.response;
        this.responsesRequire(state.phase === "awaiting" && BoundaryCapture.identity(response?.id)
          && BoundaryCapture.identity(response.model), "responses_created_shape");
        state.id = response.id;
        state.model = response.model;
        this.responsesRequire(this.responsesSnapshot(response, "in_progress").output.length === 0,
          "responses_created_output");
        state.phase = "active";
        return;
      }
      this.responsesRequire(state.phase === "active", "responses_missing_created");
      switch (frame.type) {
        case "response.in_progress":
          this.responsesRequire(this.responsesSnapshot(frame.response, "in_progress").output.length === 0,
            "responses_progress_output");
          break;
        case "response.output_item.added": {
          const item = frame.item;
          this.responsesRequire(this.responsesIndex(frame.output_index) && frame.output_index === state.items.size
            && BoundaryCapture.identity(item?.id) && ![...state.items.values()].some((known) => known.id === item.id),
          "responses_added_item_identity");
          this.responsesRequire(item.type === "message" || item.type === "reasoning", "responses_nontext_output");
          if (item.type === "message") {
            this.responsesRequire(item.role === "assistant" && item.status === "in_progress"
              && Array.isArray(item.content) && item.content.length === 0, "responses_added_message_shape");
          } else {
            this.responsesRequire((item.status === undefined || item.status === "in_progress")
              && Array.isArray(item.summary) && item.summary.length === 0, "responses_added_reasoning_shape");
          }
          state.items.set(frame.output_index, { id: item.id, kind: item.type, parts: new Map(), done: false });
          break;
        }
        case "response.content_part.added": {
          const item = this.responsesItem(frame, "message");
          this.responsesRequire(this.responsesIndex(frame.content_index) && frame.content_index === item.parts.size
            && frame.part?.type === "output_text" && frame.part.text === "", "responses_added_content_shape");
          item.parts.set(frame.content_index, { phase: "streaming", delta: "" });
          break;
        }
        case "response.output_text.delta": {
          const part = this.responsesContent(frame);
          this.responsesRequire(part.phase === "streaming", "responses_delta_after_done");
          part.delta += this.responsesText(frame.delta);
          this.textBytes += Buffer.byteLength(frame.delta);
          break;
        }
        case "response.output_text.done": {
          const part = this.responsesContent(frame);
          this.responsesRequire(part.phase === "streaming" && frame.status !== "incomplete", "responses_text_done_order");
          part.doneText = this.responsesText(frame.text);
          part.phase = "done";
          break;
        }
        case "response.content_part.done": {
          const part = this.responsesContent(frame);
          this.responsesRequire(part.phase === "done" && frame.part?.type === "output_text"
            && frame.status !== "incomplete", "responses_content_done_order");
          part.partText = this.responsesText(frame.part.text);
          part.phase = "part_done";
          break;
        }
        case "response.output_item.done": {
          const item = state.items.get(frame.output_index);
          this.responsesRequire(this.responsesIndex(frame.output_index) && item !== undefined && !item.done
            && item.id === frame.item?.id && item.kind === frame.item.type, "responses_done_item_correlation");
          if (item.kind === "message") this.responsesMessage(frame.item, item, "itemText");
          else this.responsesReasoning(frame.item);
          item.done = true;
          break;
        }
        case "response.reasoning_summary_part.added":
        case "response.reasoning_summary_part.done":
          this.responsesItem(frame, "reasoning");
          this.responsesRequire(this.responsesIndex(frame.summary_index) && frame.part?.type === "summary_text"
            && typeof frame.part.text === "string" && frame.status !== "incomplete", "responses_reasoning_part_shape");
          break;
        case "response.reasoning_summary_text.delta":
        case "response.reasoning_summary_text.done":
          this.responsesItem(frame, "reasoning");
          this.responsesRequire(this.responsesIndex(frame.summary_index)
            && typeof (frame.type.endsWith(".delta") ? frame.delta : frame.text) === "string"
            && frame.status !== "incomplete", "responses_reasoning_text_shape");
          break;
        case "response.reasoning_text.delta":
        case "response.reasoning_text.done":
          this.responsesItem(frame, "reasoning");
          this.responsesRequire(this.responsesIndex(frame.content_index)
            && typeof (frame.type.endsWith(".delta") ? frame.delta : frame.text) === "string"
            && frame.status !== "incomplete", "responses_reasoning_text_shape");
          break;
        case "response.completed": {
          const response = this.responsesSnapshot(frame.response, "completed");
          this.responsesRequire(response.output.length === state.items.size && state.items.size > 0,
            "responses_completed_inventory");
          for (const [index, item] of response.output.entries()) {
            const observed = state.items.get(index);
            this.responsesRequire(observed?.done && item?.id === observed.id && item.type === observed.kind,
              "responses_completed_item_correlation");
            if (item.type === "message") this.responsesMessage(item, observed, "finalText");
            else this.responsesReasoning(item);
          }
          this.responsesRequire([...state.items.values()].some((item) => item.kind === "message"),
            "responses_missing_text_message");
          state.phase = "completed";
          this.finish.add("stop");
          break;
        }
        default:
          this.responsesRequire(false, "responses_unsupported_event");
      }
    } catch (error) {
      this.bound(error instanceof Error && /^responses_[a-z_]+$/.test(error.message)
        ? error.message : "responses_observer_error");
    }
  }

  responsesEnd(outcome) {
    const state = this.responses;
    const complete = this.complete && this.invalidFrames === 0 && outcome === "eof"
      && this.status >= 200 && this.status < 300 && this.format === "sse" && state.phase === "completed";
    const parts = complete ? [...state.items].sort(([left], [right]) => left - right)
      .flatMap(([output_index, item]) => item.kind === "message"
        ? [...item.parts].sort(([left], [right]) => left - right).map(([content_index, part]) => ({ output_index, content_index, ...part })) : []) : [];
    const snapshots = {
      done: parts.map((part) => part.doneText).join(""),
      part: parts.map((part) => part.partText).join(""),
      item: parts.map((part) => part.itemText).join(""),
      final: parts.map((part) => part.finalText).join(""),
      model: state.model,
      parts,
    };
    this.owner.freshResponse(this.flow, complete, parts.map((part) => part.delta).join(""),
      "responses_terminal_or_capture_shape", snapshots);
    state.items.clear();
  }

  static knownUsage(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const counts = ["prompt_tokens", "completion_tokens", "total_tokens"];
    const details = ["prompt_tokens_details", "completion_tokens_details"];
    const detailCounts = ["cached_tokens", "audio_tokens", "reasoning_tokens", "accepted_prediction_tokens", "rejected_prediction_tokens", "text_tokens"];
    return Object.keys(value).every((key) => counts.includes(key) || details.includes(key))
      && counts.every((key) => Number.isSafeInteger(value[key]) && value[key] >= 0)
      && details.every((key) => value[key] === undefined || value[key] === null
        || (typeof value[key] === "object" && !Array.isArray(value[key])
          && Object.entries(value[key]).every(([name, count]) => detailCounts.includes(name) && Number.isSafeInteger(count) && count >= 0)));
  }

  freshFrame(frame) {
    if (this.freshDone || !frame || typeof frame !== "object" || Array.isArray(frame)
      || Object.hasOwn(frame, "error") || !Array.isArray(frame.choices)) return false;
    if (!frame.choices.length) return BoundaryResponse.knownUsage(frame.usage);
    if (this.freshTerminalSeen || frame.choices.length !== 1) return false;
    const choice = frame.choices[0];
    if (!choice || typeof choice !== "object" || Array.isArray(choice)
      || (choice.index !== undefined && choice.index !== 0)) return false;
    if (choice.delta !== undefined && choice.message !== undefined) return false;
    const delta = choice.delta ?? choice.message;
    if (!delta || typeof delta !== "object" || Array.isArray(delta)
      || (delta.role !== undefined && delta.role !== "assistant")
      || (delta.content !== undefined && delta.content !== null && typeof delta.content !== "string")
      || Object.hasOwn(delta, "function_call")
      || (delta.tool_calls !== undefined && delta.tool_calls !== null && !Array.isArray(delta.tool_calls))) return false;
    if (choice.finish_reason !== undefined && choice.finish_reason !== null) {
      this.freshTerminalSeen = true;
      if (choice.finish_reason !== "stop") return false;
    }
    return true;
  }

  frame(text) {
    if (this.responses && this.fresh && !this.complete) return;
    if (this.responses && this.fresh && text === "[DONE]") {
      this.bound("responses_unexpected_done_sentinel");
      return;
    }
    if (text === "[DONE]") {
      if (this.fresh && this.freshDone) this.freshTextComplete = false;
      this.freshDone = true;
      return;
    }
    if (++this.frames > 16384) return this.bound("frame_count_bound");
    let frame;
    try {
      frame = JSON.parse(text);
    } catch {
      this.invalidFrames++;
      return;
    }
    if (this.responses && this.fresh) {
      this.responsesFrame(frame);
      return;
    }
    if (this.fresh && !this.freshFrame(frame)) {
      this.freshTextComplete = false;
      this.freshText = "";
    }
    if (!Array.isArray(frame.choices)) return;
    if (frame.choices.length > 4) return this.bound("choice_bound");
    for (const choice of frame.choices) {
      const delta = choice.delta ?? choice.message;
      if (this.fresh) {
        if (this.freshTextComplete && typeof delta?.content === "string") {
          this.freshTextBytes += Buffer.byteLength(delta.content);
          if (this.freshTextBytes > BoundaryCapture.FRESH_TEXT_LIMIT) {
            this.freshTextComplete = false;
            this.freshText = "";
          } else this.freshText += delta.content;
        }
      }
      if (typeof delta?.content === "string") this.textBytes += Buffer.byteLength(delta.content);
      if (Array.isArray(delta?.tool_calls)) {
        if (delta.tool_calls.length > 16) return this.bound("tool_call_bound");
        for (const [position, value] of delta.tool_calls.entries()) {
          const index = `${Number.isInteger(choice.index) ? choice.index : 0}:${Number.isInteger(value.index) ? value.index : position}`;
          if (!this.calls.has(index)) {
            if (this.calls.size >= 16) return this.bound("tool_call_bound");
            this.calls.set(index, { name: "", arguments: "", malformed: false });
          }
          const call = this.calls.get(index);
          if (typeof value.function?.name === "string") call.name += value.function.name;
          if (typeof value.function?.arguments === "string") call.arguments += value.function.arguments;
          else if (value.function && Object.hasOwn(value.function, "arguments")) call.malformed = true;
          if (call.name.length > 128 || call.arguments.length > BoundaryCapture.ARGUMENT_LIMIT) return this.bound("tool_argument_bound");
        }
      }
      if (choice.finish_reason != null) {
        this.finish.add(["stop", "length", "tool_calls", "function_call", "content_filter"].includes(choice.finish_reason) ? choice.finish_reason : "other");
      }
    }
  }

  end(outcome) {
    if (this.done) return;
    this.done = true;
    try {
      if (outcome !== "eof" && outcome !== "no_body") this.complete = false;
      if (this.complete && outcome === "eof") {
        this.pending += this.decoder.decode();
        if (this.format === "json") this.frame(this.pending);
        else if (this.pending.length || this.data.length) this.complete = false;
      }
      const questions = [...this.calls.values()].filter((call) => call.name === "question");
      const shapes = questions.map((call) => {
        if (call.malformed) return { arguments_json: false };
        try {
          const args = JSON.parse(call.arguments);
          const list = args?.questions;
          return {
            arguments_json: true, questions_shape: BoundaryCapture.shape(list),
            question_count: Array.isArray(list) ? list.length : null,
            typed_question_fields: Array.isArray(list) && list.length <= 64 && list.every((q) =>
              q && typeof q.question === "string" && typeof q.header === "string" && Array.isArray(q.options)
              && q.options.every((o) => o && typeof o.label === "string" && typeof o.description === "string")
              && (!Object.hasOwn(q, "multiple") || typeof q.multiple === "boolean")
              && (!Object.hasOwn(q, "custom") || typeof q.custom === "boolean")),
          };
        } catch {
          return { arguments_json: false };
        }
      });
      const parsed = this.complete && this.invalidFrames === 0
        && (!this.responses || (this.fresh && this.responses.phase === "completed"));
      this.owner.emit({ event: "response_end", flow: this.flow, outcome, capture: parsed ? "complete" : "incomplete",
        ...(this.responses ? { target_prompt: this.fresh, provider_terminal_observed: this.responses.phase === "completed" } : {}),
        frames: this.frames, invalid_json_frames: this.invalidFrames, text_bytes: this.textBytes,
        structured_tool_call_count: this.calls.size, question_call_count: questions.length,
        question_argument_shapes: shapes, finish_reasons: [...this.finish] });
    } catch {
      this.owner.emit({ event: "response_end", flow: this.flow, outcome, capture: "incomplete" });
      this.complete = false;
    }
    if (this.fresh && this.responses) {
      this.stageEnd(outcome);
      this.responsesEnd(outcome);
    } else if (this.fresh) {
      const complete = this.complete && this.invalidFrames === 0 && outcome === "eof"
        && this.freshTextComplete && this.status >= 200 && this.status < 300
        && this.finish.size === 1 && this.finish.has("stop") && this.calls.size === 0;
      this.owner.freshResponse(this.flow, complete, this.freshText, "provider_terminal_or_text_shape");
    }
    this.freshText = "";
    this.pending = "";
    this.data = [];
    this.calls.clear();
  }
}

export default async function opencodeBoundary(_input, options) {
  let owner;
  try {
    owner = new BoundaryCapture(options);
  } catch {
    return {};
  }
  return {
    config: async (config) => owner.install(config),
    "chat.message": async (input, output) => owner.chatMessage(input, output),
    event: async (input) => owner.nativeEvent(input),
    "experimental.text.complete": async (input, output) => owner.nativeText(input, output),
    dispose: async () => owner.freshSummary(true),
  };
}
