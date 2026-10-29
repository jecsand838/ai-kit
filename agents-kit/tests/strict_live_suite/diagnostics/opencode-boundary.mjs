import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, openSync, closeSync } from "node:fs";
import { join } from "node:path";

// Test-only native plugin. No raw request, response, identifier, header, URL,
// description, prompt, or argument value enters a receipt.
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
    this.flow = 0;
    this.written = 0;
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
        line = JSON.stringify({ version: 1, capture_id: this.capture, event: "observer_incomplete", flow: facts.flow, reason: "receipt_record_bound" }) + "\n";
      }
      if (this.written + Buffer.byteLength(line) > BoundaryCapture.RECEIPT_LIMIT - 256) {
        this.receiptsExhausted = true;
        line = JSON.stringify({ version: 1, capture_id: this.capture, event: "observer_incomplete", reason: "receipt_total_bound" }) + "\n";
      }
      appendFileSync(this.path, line, { encoding: "utf8", mode: 0o600 });
      this.written += Buffer.byteLength(line);
    } catch {
      // Diagnostics never replace a native request, stream, or cleanup outcome.
    }
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
        || input.model?.providerID !== "ollama" || input.model?.modelID !== this.expectedNativeModel
        || message.model?.providerID !== "ollama" || message.model?.modelID !== this.expectedNativeModel) {
        this.freshIncomplete("user_ownership");
        return;
      }
      if (this.fresh.root) {
        this.freshIncomplete("duplicate_fresh_root");
        return;
      }
      this.fresh.root = { session: input.sessionID, user: message.id };
      this.emit({ event: "fresh_user", capture: "complete", prompt_matches_expected: true,
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
      if (!BoundaryCapture.identity(info.id) || info.providerID !== "ollama" || info.modelID !== this.expectedNativeModel) {
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
    if (!Array.isArray(request.messages) || request.messages.length > 256) {
      this.freshIncomplete("request_messages_shape");
      return false;
    }
    const user = request.messages.findLast((message) => message?.role === "user");
    if (!user) return false;
    let texts;
    if (typeof user.content === "string") texts = [user.content];
    else if (Array.isArray(user.content) && user.content.length <= BoundaryCapture.FRESH_PART_LIMIT
      && user.content.every((part) => part?.type === "text" && typeof part.text === "string")) {
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
      temperature_present: temperaturePresent, temperature_numeric: temperatureNumeric,
      temperature_value: temperatureNumeric ? request.temperature : null,
      temperature_matches_scenario: temperatureExpected ? temperatureNumeric && request.temperature === this.options.expectedTemperature : null });
    if (matches !== 1 || !modelMatches || !sessionMatches) {
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

  freshResponse(flow, complete, text, reason) {
    const response = this.fresh?.flows.get(flow);
    if (!response) return;
    response.complete = complete;
    response.text = complete ? text : "";
    this.emit({ event: "fresh_provider_text", flow, capture: complete ? "complete" : "incomplete",
      ...(complete ? { text_bytes: Buffer.byteLength(text), text_equals_expected: text === this.fresh.expected,
        text_equals_asterisk_wrapped_expected: text === `*${this.fresh.expected}*` } : { reason }),
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
      foreground_http_body_observed: false,
      ...(complete ? {
        provider_equals_expected: flows[0].text === this.fresh.expected,
        provider_equals_asterisk_wrapped_expected: flows[0].text === `*${this.fresh.expected}*`,
        native_equals_expected: native === this.fresh.expected, native_equals_provider: native === flows[0].text,
        native_text_parts: assistants[0].parts.size, native_text_bytes: Buffer.byteLength(native),
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
    const provider = config.provider?.ollama;
    if (provider?.npm !== "@ai-sdk/openai-compatible" || !provider.options) {
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
    this.emit({ event: "installed", boundary: "serialized_provider_http", expected_role: this.options.expectedRole === "opencode_default" ? "opencode_default" : "opencode_shell", flow_limit: BoundaryCapture.FLOW_LIMIT });
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
          const tools = Array.isArray(request.tools) ? request.tools : [];
          const questions = tools.filter((tool) => tool?.type === "function" && tool.function?.name === "question");
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
            const schema = questions[0].function.parameters;
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
    this.owner.emit({ event: "response_incomplete", flow: this.flow, reason });
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
      this.owner.emit({ event: "response_end", flow: this.flow, outcome, capture: this.complete && this.invalidFrames === 0 ? "complete" : "incomplete",
        frames: this.frames, invalid_json_frames: this.invalidFrames, text_bytes: this.textBytes,
        structured_tool_call_count: this.calls.size, question_call_count: questions.length,
        question_argument_shapes: shapes, finish_reasons: [...this.finish] });
    } catch {
      this.owner.emit({ event: "response_end", flow: this.flow, outcome, capture: "incomplete" });
      this.complete = false;
    }
    if (this.fresh) {
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
