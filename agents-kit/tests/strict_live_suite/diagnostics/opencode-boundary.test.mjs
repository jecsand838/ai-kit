import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, renameSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import plugin from "./opencode-boundary.mjs";

class CaptureFixture {
  constructor(delegate) {
    this.directory = mkdtempSync(join(tmpdir(), "agent-opencode-boundary-"));
    this.config = { provider: { ollama: {
      npm: "@ai-sdk/openai-compatible", models: { "PRIVATE_MODEL": { name: "Expected Model" } },
      options: { fetch: delegate, baseURL: "http://127.0.0.1:11434/v1", apiKey: "PRIVATE_KEY", chunkTimeout: 1000 },
    } } };
  }

  async install() {
    const hooks = await plugin({}, { outputDirectory: this.directory, expectedModelName: "Expected Model" });
    await hooks.config(this.config);
    return this.config.provider.ollama.options.fetch;
  }

  request() {
    return { method: "POST", headers: { authorization: "PRIVATE_AUTH" }, body: JSON.stringify({
      model: "PRIVATE_MODEL", stream: true, messages: [{ role: "user", content: "PRIVATE_PROMPT" }],
      tools: [{ type: "function", function: { name: "question", description: "PRIVATE_DESCRIPTION", parameters: {
        type: "object", required: ["questions"], properties: { questions: { type: "array", items: {
          type: "object", required: ["question", "header", "options"], properties: {
            question: { type: "string", description: "PRIVATE_SCHEMA_DESCRIPTION" }, header: { type: "string" },
            options: { type: "array", items: { type: "object", properties: { label: { type: "string" }, description: { type: "string" } } } },
            multiple: { type: "boolean" },
          },
        } } },
      } } }], tool_choice: "auto",
    }) };
  }

  facts() {
    return readdirSync(this.directory).flatMap((name) => {
      assert.equal(statSync(join(this.directory, name)).mode & 0o777, 0o600);
      const text = readFileSync(join(this.directory, name), "utf8");
      assert(!text.includes("PRIVATE_"), "all raw private values must stay absent");
      return text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
    });
  }

  close() { rmSync(this.directory, { recursive: true, force: true }); }

  static stream(bytes, width = 7) {
    let at = 0;
    return new ReadableStream({ pull(controller) {
      if (at === bytes.length) return controller.close();
      controller.enqueue(bytes.subarray(at, at + width));
      at = Math.min(bytes.length, at + width);
    } }, { highWaterMark: 0 });
  }
}

test("fragmented SSE preserves exact request, receiver, response metadata and bytes while redacting tool evidence", async () => {
  const argumentsText = JSON.stringify({ questions: [{ question: "PRIVATE_QUESTION", header: "PRIVATE_HEADER", options: [{ label: "PRIVATE_LABEL", description: "PRIVATE_OPTION_DESCRIPTION" }], multiple: false }] });
  const frames = [
    { choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "PRIVATE_CALL_ID", type: "function", function: { name: "que", arguments: argumentsText.slice(0, 17) } }] }, finish_reason: null }] },
    { choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { name: "stion", arguments: argumentsText.slice(17) } }] }, finish_reason: "tool_calls" }] },
  ];
  const bytes = new TextEncoder().encode(frames.map((frame) => `data: ${JSON.stringify(frame)}\r\n\r\n`).join("") + "data: [DONE]\n\n");
  const receiver = {};
  let supplied;
  const fixture = new CaptureFixture(function (...args) {
    assert.equal(this, receiver);
    assert.equal(args[0], supplied[0]);
    assert.equal(args[1], supplied[1]);
    const response = new Response(CaptureFixture.stream(bytes), { status: 201, statusText: "Created", headers: { "content-type": "text/event-stream", "x-private": "PRIVATE_RESPONSE_HEADER" } });
    Object.defineProperty(response, "url", { value: "http://127.0.0.1/PRIVATE_URL" });
    Object.defineProperty(response, "redirected", { value: true });
    return response;
  });
  try {
    const fetch = await fixture.install();
    supplied = ["http://127.0.0.1:11434/v1/chat/completions", fixture.request()];
    const before = JSON.stringify(supplied[1]);
    const response = await Reflect.apply(fetch, receiver, supplied);
    assert.equal(response.status, 201);
    assert.equal(response.statusText, "Created");
    assert.equal(response.url, "http://127.0.0.1/PRIVATE_URL");
    assert.equal(response.redirected, true);
    assert.equal(response.type, "default");
    assert.equal(response.headers.get("x-private"), "PRIVATE_RESPONSE_HEADER");
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), bytes);
    assert.equal(JSON.stringify(supplied[1]), before);
    const facts = fixture.facts();
    const request = facts.find((fact) => fact.event === "request");
    assert.equal(request.model_matches_configured, true);
    assert.equal(request.question_definition_count, 1);
    assert.equal(request.question_schema.properties.questions.type, "array");
    assert.match(request.question_schema_sha256, /^[a-f0-9]{64}$/);
    const end = facts.find((fact) => fact.event === "response_end");
    assert.equal(end.question_call_count, 1);
    assert.equal(end.question_argument_shapes[0].typed_question_fields, true);
    assert.deepEqual(end.finish_reasons, ["tool_calls"]);
    assert.equal(end.capture, "complete");
  } finally { fixture.close(); }
});

test("fragmented JSON distinguishes provider text from structured tool calls", async () => {
  const text = JSON.stringify({ choices: [{ message: { role: "assistant", content: "PRIVATE_TEXT <tool_call>question</tool_call>" }, finish_reason: "stop" }] });
  const fixture = new CaptureFixture(() => new Response(CaptureFixture.stream(new TextEncoder().encode(text), 1), { headers: { "content-type": "application/json" } }));
  try {
    const fetch = await fixture.install();
    assert.equal(await (await fetch("unused", fixture.request())).text(), text);
    const end = fixture.facts().find((fact) => fact.event === "response_end");
    assert.equal(end.structured_tool_call_count, 0);
    assert(end.text_bytes > 0);
    assert.deepEqual(end.finish_reasons, ["stop"]);
  } finally { fixture.close(); }
});

test("malformed and oversized frames remain exact passthrough and incomplete evidence", async () => {
  for (const text of ["data: {PRIVATE_MALFORMED}\n\n", `data: ${"PRIVATE_OVERSIZE".repeat(20000)}\n\n`]) {
    const fixture = new CaptureFixture(() => new Response(text, { headers: { "content-type": "text/event-stream" } }));
    try {
      const fetch = await fixture.install();
      assert.equal(await (await fetch("unused", fixture.request())).text(), text);
      const end = fixture.facts().find((fact) => fact.event === "response_end");
      assert.equal(end.capture, "incomplete");
    } finally { fixture.close(); }
  }
});

test("choice and tool-call bounds cannot hide a Question behind complete evidence", async () => {
  const question = { function: { name: "question", arguments: JSON.stringify({ questions: [{ question: "PRIVATE_OMITTED_QUESTION" }] }) } };
  const boundedFrames = [
    { reason: "choice_bound", frame: { choices: [
      ...Array.from({ length: 4 }, (_, index) => ({ index, message: { content: "PRIVATE_TEXT" }, finish_reason: "stop" })),
      { index: 4, message: { tool_calls: [question] }, finish_reason: "tool_calls" },
    ] } },
    { reason: "tool_call_bound", frame: { choices: [{ index: 0, message: { tool_calls: [
      ...Array.from({ length: 16 }, (_, index) => ({ index, function: { name: "PRIVATE_OTHER_TOOL", arguments: "{}" } })),
      { index: 16, ...question },
    ] }, finish_reason: "tool_calls" }] } },
  ];
  for (const { reason, frame } of boundedFrames) {
    for (const format of ["json", "sse"]) {
      const text = format === "sse" ? `data: ${JSON.stringify(frame)}\n\ndata: [DONE]\n\n` : JSON.stringify(frame);
      const bytes = new TextEncoder().encode(text);
      const fixture = new CaptureFixture(() => new Response(CaptureFixture.stream(bytes, 3), {
        headers: { "content-type": format === "sse" ? "text/event-stream" : "application/json" },
      }));
      try {
        const fetch = await fixture.install();
        const response = await fetch("unused", fixture.request());
        assert.deepEqual(new Uint8Array(await response.arrayBuffer()), bytes);
        const facts = fixture.facts();
        const end = facts.find((fact) => fact.event === "response_end");
        assert.equal(end.capture, "incomplete", `${format} ${reason} must not claim complete Question evidence`);
        assert(facts.some((fact) => fact.event === "response_incomplete" && fact.reason === reason));
      } finally { fixture.close(); }
    }
  }
});

test("oversized request and unknown structured argument shapes never leak raw values", async () => {
  const body = JSON.stringify({ choices: [{ message: { tool_calls: [{ function: { name: "question", arguments: "PRIVATE_INVALID_ARGUMENT_JSON" } }] }, finish_reason: "tool_calls" }] });
  const fixture = new CaptureFixture(() => new Response(body));
  try {
    const fetch = await fixture.install();
    const request = fixture.request();
    request.body = "PRIVATE_OVERSIZE_REQUEST".repeat(100000);
    assert.equal(await (await fetch("unused", request)).text(), body);
    const facts = fixture.facts();
    assert(facts.some((fact) => fact.reason === "request_bound"));
    assert.equal(facts.find((fact) => fact.event === "response_end").question_argument_shapes[0].arguments_json, false);
  } finally { fixture.close(); }
});

test("no eager response read; cancellation reason reaches the native body unchanged", async () => {
  let pulls = 0;
  let cancelled;
  const fixture = new CaptureFixture(() => new Response(new ReadableStream({
    pull(controller) { pulls++; controller.enqueue(new TextEncoder().encode("data: ")); },
    cancel(reason) { cancelled = reason; },
  }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } }));
  try {
    const fetch = await fixture.install();
    const response = await fetch("unused", fixture.request());
    assert.equal(pulls, 0);
    const reader = response.body.getReader();
    await reader.read();
    assert.equal(pulls, 1);
    const reason = { private: "PRIVATE_CANCELLATION" };
    await reader.cancel(reason);
    assert.equal(cancelled, reason);
    assert.equal(fixture.facts().find((fact) => fact.event === "response_end").outcome, "cancelled");
  } finally { fixture.close(); }
});

test("native fetch and body errors retain exact identity", async () => {
  for (const phase of ["fetch", "body"]) {
    const error = new Error("PRIVATE_NATIVE_ERROR");
    const fixture = new CaptureFixture(() => {
      if (phase === "fetch") throw error;
      return new Response(new ReadableStream({ pull(controller) { controller.error(error); } }, { highWaterMark: 0 }));
    });
    try {
      const fetch = await fixture.install();
      await assert.rejects(async () => (await fetch("unused", fixture.request())).text(), (actual) => actual === error);
      fixture.facts();
    } finally { fixture.close(); }
  }
});

test("observer output failure does not alter the request or response outcome", async () => {
  const fixture = new CaptureFixture(() => new Response("PRIVATE_BODY"));
  try {
    const fetch = await fixture.install();
    fixture.close();
    assert.equal(await (await fetch("unused", fixture.request())).text(), "PRIVATE_BODY");
  } finally { fixture.close(); }
});

test("flow count is bounded while later requests retain native responses", async () => {
  let calls = 0;
  const fixture = new CaptureFixture(() => { calls++; return new Response("{}"); });
  try {
    const fetch = await fixture.install();
    for (let index = 0; index < 20; index++) assert.equal(await (await fetch("unused", fixture.request())).text(), "{}");
    assert.equal(calls, 20);
    assert.equal(fixture.facts().filter((fact) => fact.event === "request").length, 16);
  } finally { fixture.close(); }
});

test("request facts distinguish absent Question declaration from malformed schema without leaking values", async () => {
  for (const variant of ["absent", "malformed"]) {
    const fixture = new CaptureFixture(() => new Response("{}"));
    try {
      const fetch = await fixture.install();
      const request = fixture.request();
      const data = JSON.parse(request.body);
      if (variant === "absent") data.tools = [];
      else data.tools[0].function.parameters = "PRIVATE_INVALID_SCHEMA";
      request.body = JSON.stringify(data);
      await (await fetch("unused", request)).text();
      const facts = fixture.facts().find((fact) => fact.event === "request");
      assert.equal(facts.question_definition_count, variant === "absent" ? 0 : 1);
      if (variant === "malformed") assert.deepEqual(facts.question_schema, { shape: "string" });
    } finally { fixture.close(); }
  }
});

test("cancellation during an outstanding native read settles that same read", async () => {
  let cancelled;
  const fixture = new CaptureFixture(() => new Response(new ReadableStream({
    pull() {}, cancel(reason) { cancelled = reason; },
  }, { highWaterMark: 0 })));
  try {
    const fetch = await fixture.install();
    const response = await fetch("unused", fixture.request());
    const reader = response.body.getReader();
    const pending = reader.read();
    const reason = { private: "PRIVATE_PENDING_CANCEL" };
    await reader.cancel(reason);
    assert.deepEqual(await pending, { done: true, value: undefined });
    assert.equal(cancelled, reason);
    fixture.facts();
  } finally { fixture.close(); }
});

class FreshFixture extends CaptureFixture {
  static EXPECTED = "PRIVATE_FRESH_TOKEN";
  static PROMPT = "Do not call any tool. Reply with exactly PRIVATE_FRESH_TOKEN and nothing else.";

  async install(options = {}) {
    this.hooks = await plugin({}, {
      outputDirectory: this.directory, expectedModelName: "Expected Model",
      freshOutput: { prompt: FreshFixture.PROMPT, expected: FreshFixture.EXPECTED },
      ...options,
    });
    await this.hooks.config(this.config);
    return this.config.provider.ollama.options.fetch;
  }

  request() {
    const request = super.request();
    request.headers["x-opencode-session-id"] = "PRIVATE_SESSION";
    const body = JSON.parse(request.body);
    body.messages = [
      { role: "user", content: "PRIVATE_OLD_QUESTION_PROMPT" },
      { role: "assistant", content: "PRIVATE_OLD_ANSWER" },
      { role: "user", content: FreshFixture.PROMPT },
    ];
    request.body = JSON.stringify(body);
    return request;
  }

  async root(id = "PRIVATE_USER") {
    const input = { sessionID: "PRIVATE_SESSION", messageID: id, model: { providerID: "ollama", modelID: this.nativeModel ?? "PRIVATE_MODEL" } };
    const output = {
      message: { id, sessionID: "PRIVATE_SESSION", role: "user", agent: "build", model: { ...input.model } },
      parts: [{ id: "PRIVATE_USER_PART", sessionID: "PRIVATE_SESSION", messageID: id, type: "text", text: FreshFixture.PROMPT }],
    };
    const before = JSON.stringify({ input, output });
    await this.hooks["chat.message"](input, output);
    assert.equal(JSON.stringify({ input, output }), before);
  }

  async assistant({ id = "PRIVATE_ASSISTANT", parent = "PRIVATE_USER", completed = false, finish = "stop", model = this.nativeModel ?? "PRIVATE_MODEL" } = {}) {
    await this.hooks.event({ event: { type: "message.updated", properties: { info: {
      id, sessionID: "PRIVATE_SESSION", parentID: parent, role: "assistant",
      providerID: "ollama", modelID: model,
      time: completed ? { created: 1, completed: 2 } : { created: 1 },
      ...(completed ? { finish } : {}),
    } } } });
  }

  async native(text, part = "PRIVATE_PART_1", message = "PRIVATE_ASSISTANT") {
    const input = { sessionID: "PRIVATE_SESSION", messageID: message, partID: part };
    const output = { text };
    await this.hooks["experimental.text.complete"](input, output);
    assert.equal(output.text, text, "native text hook must remain read-only");
  }

  async finish() {
    await this.hooks.dispose?.();
    return this.facts().findLast((fact) => fact.event === "fresh_summary");
  }

  static response(text) {
    const body = `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: text }, finish_reason: "stop" }] })}\n\ndata: [DONE]\n\n`;
    return { body, response: new Response(CaptureFixture.stream(new TextEncoder().encode(body), 3), { headers: { "content-type": "text/event-stream" } }) };
  }
}

test("upfront temperature receipts detect missing, wrong and malformed values without changing requests", async () => {
  for (const value of [0, 1, "0", null, undefined]) {
    let request;
    let original;
    const fixture = new FreshFixture((_url, init) => {
      assert.equal(init, request);
      assert.equal(init.body, original);
      return new Response(JSON.stringify({ choices: [{ message: { role: "assistant", content: FreshFixture.EXPECTED }, finish_reason: "stop" }] }), {
        headers: { "content-type": "application/json" },
      });
    });
    try {
      const fetch = await fixture.install({ expectedTemperature: 0, expectedRole: "opencode_default" });
      await fixture.root();
      request = fixture.request();
      const body = JSON.parse(request.body);
      if (value !== undefined) body.temperature = value;
      request.body = JSON.stringify(body);
      original = request.body;
      const response = await fetch("http://127.0.0.1:11434/v1/chat/completions", request);
      await response.text();
      assert.equal(request.body, original);
      const fact = fixture.facts().find((item) => item.event === "fresh_request");
      assert.equal(fact.capture, "complete");
      assert.equal(fact.temperature_present, value !== undefined);
      assert.equal(fact.temperature_numeric, typeof value === "number");
      assert.equal(fact.temperature_value, typeof value === "number" ? value : null);
      assert.equal(fact.temperature_matches_scenario, value === 0);
    } finally { fixture.close(); }
  }
});

test("fresh provenance distinguishes provider formatting from later native alteration without revealing text", async () => {
  for (const [provider, native] of [
    [`*${FreshFixture.EXPECTED}*`, `*${FreshFixture.EXPECTED}*`],
    [FreshFixture.EXPECTED, `*${FreshFixture.EXPECTED}*`],
    [`*${FreshFixture.EXPECTED}*`, FreshFixture.EXPECTED],
  ]) {
    const { body, response } = FreshFixture.response(provider);
    const fixture = new FreshFixture(() => response);
    try {
      const fetch = await fixture.install();
      await fixture.root();
      await fixture.assistant();
      assert.equal(await (await fetch("unused", fixture.request())).text(), body);
      await fixture.native(native);
      await fixture.assistant({ completed: true });
      const summary = await fixture.finish();
      assert.equal(summary.capture, "complete");
      assert.equal(summary.provider_equals_expected, provider === FreshFixture.EXPECTED);
      assert.equal(summary.provider_equals_asterisk_wrapped_expected, provider === `*${FreshFixture.EXPECTED}*`);
      assert.equal(summary.native_equals_expected, native === FreshFixture.EXPECTED);
      assert.equal(summary.native_equals_provider, native === provider);
      assert.equal(summary.foreground_http_body_observed, false);
      assert.equal(summary.native_text_sha256, undefined, "Ollama text fingerprints remain outside this cloud-only diagnostic scope");
      assert.equal(summary.native_parts, undefined);
      assert.equal(fixture.facts().find((fact) => fact.event === "fresh_user").native_session_sha256, undefined);
    } finally { fixture.close(); }
  }
});

test("fresh comparison accepts supported text arrays and orders native parts by identity in either observation order", async () => {
  const { body, response } = FreshFixture.response(FreshFixture.EXPECTED);
  const fixture = new FreshFixture(() => response);
  try {
    const fetch = await fixture.install();
    await fixture.root();
    await fixture.assistant();
    await fixture.native(FreshFixture.EXPECTED.slice(7), "PRIVATE_PART_2");
    await fixture.native(FreshFixture.EXPECTED.slice(0, 7), "PRIVATE_PART_1");
    await fixture.assistant({ completed: true });
    const request = fixture.request();
    const value = JSON.parse(request.body);
    value.messages.at(-1).content = [{ type: "text", text: FreshFixture.PROMPT }, { type: "text", text: "PRIVATE_NATIVE_REMINDER" }];
    request.body = JSON.stringify(value);
    assert.equal(await (await fetch("unused", request)).text(), body);
    const summary = await fixture.finish();
    assert.equal(summary.capture, "complete");
    assert.equal(summary.native_equals_provider, true);
    assert.equal(summary.native_text_parts, 2);
  } finally { fixture.close(); }
});

test("fresh text, assistant ownership and provider EOF may arrive in either order", async () => {
  for (const order of ["text_first", "terminal_first", "eof_first"]) {
    const { body, response } = FreshFixture.response(FreshFixture.EXPECTED);
    const fixture = new FreshFixture(() => response);
    try {
      const fetch = await fixture.install(); await fixture.root();
      if (order === "text_first") await fixture.native(FreshFixture.EXPECTED);
      if (order === "terminal_first") await fixture.assistant({ completed: true });
      assert.equal(await (await fetch("unused", fixture.request())).text(), body);
      if (order !== "text_first") await fixture.native(FreshFixture.EXPECTED);
      if (order !== "terminal_first") await fixture.assistant({ completed: true });
      const summary = await fixture.finish();
      assert.equal(summary.capture, "complete", order);
      assert.equal(summary.native_equals_provider, true, order);
    } finally { fixture.close(); }
  }
});

test("fresh chat.message model and part ownership must match the actual selected user", async () => {
  for (const variant of ["model", "provider", "part_owner", "native_model", "assistant_model"]) {
    const fixture = new FreshFixture(() => FreshFixture.response(FreshFixture.EXPECTED).response);
    try {
      const fetch = await fixture.install();
      await fixture.hooks["chat.message"]({ sessionID: "PRIVATE_SESSION", model: {
        providerID: variant === "provider" ? "PRIVATE_OTHER_PROVIDER" : "ollama",
        modelID: variant === "model" ? "PRIVATE_OTHER_MODEL" : "PRIVATE_MODEL",
      } }, { message: { id: "PRIVATE_USER", sessionID: "PRIVATE_SESSION", role: "user", model: {
        providerID: "ollama", modelID: variant === "native_model" ? "PRIVATE_OTHER_MODEL" : "PRIVATE_MODEL",
      } }, parts: [{
        id: "PRIVATE_USER_PART", sessionID: "PRIVATE_SESSION", type: "text", text: FreshFixture.PROMPT,
        messageID: variant === "part_owner" ? "PRIVATE_OTHER_USER" : "PRIVATE_USER",
      }] });
      const model = variant === "assistant_model" ? "PRIVATE_OTHER_MODEL" : "PRIVATE_MODEL";
      await fixture.assistant({ model });
      await (await fetch("unused", fixture.request())).text();
      await fixture.native(FreshFixture.EXPECTED); await fixture.assistant({ model, completed: true });
      assert.equal((await fixture.finish()).capture, "incomplete", variant);
    } finally { fixture.close(); }
  }
});

test("configured native model identity and serialized provider identity remain distinct", async () => {
  const fixture = new FreshFixture(() => FreshFixture.response(FreshFixture.EXPECTED).response);
  fixture.nativeModel = "PRIVATE_NATIVE_ALIAS";
  fixture.config.provider.ollama.models = { [fixture.nativeModel]: { name: "Expected Model", id: "PRIVATE_MODEL" } };
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    await (await fetch("unused", fixture.request())).text();
    await fixture.native(FreshFixture.EXPECTED); await fixture.assistant({ completed: true });
    assert.equal((await fixture.finish()).capture, "complete");
  } finally { fixture.close(); }
});

test("auxiliary and old Question requests do not inherit fresh-output attribution", async () => {
  const fixture = new FreshFixture(() => FreshFixture.response(FreshFixture.EXPECTED).response);
  try {
    const fetch = await fixture.install();
    await fixture.root();
    await fixture.assistant();
    const auxiliary = fixture.request();
    const value = JSON.parse(auxiliary.body);
    value.messages.push({ role: "user", content: "PRIVATE_TITLE_REQUEST" });
    auxiliary.body = JSON.stringify(value);
    await (await fetch("unused", auxiliary)).text();
    await (await fetch("unused", fixture.request())).text();
    await fixture.native(FreshFixture.EXPECTED);
    await fixture.assistant({ completed: true });
    const summary = await fixture.finish();
    assert.equal(summary.capture, "complete");
    assert.equal(summary.matching_provider_flows, 1);
    assert.equal(summary.native_equals_provider, true);
  } finally { fixture.close(); }
});

test("wrong model, session, parent, duplicate roots and ambiguous flows cannot certify fresh provenance", async () => {
  for (const variant of ["model", "session", "parent", "root", "flows", "duplicate_part", "missing_hook"]) {
    const fixture = new FreshFixture(() => FreshFixture.response(FreshFixture.EXPECTED).response);
    try {
      const fetch = await fixture.install();
      await fixture.root();
      if (variant === "root") await fixture.root("PRIVATE_SECOND_USER");
      const parent = variant === "parent" ? "PRIVATE_OLD_USER" : "PRIVATE_USER";
      await fixture.assistant({ parent });
      const request = fixture.request();
      if (variant === "session") request.headers["x-opencode-session-id"] = "PRIVATE_OTHER_SESSION";
      if (variant === "model") {
        const value = JSON.parse(request.body); value.model = "PRIVATE_OTHER_MODEL"; request.body = JSON.stringify(value);
      }
      await (await fetch("unused", request)).text();
      if (variant === "flows") await (await fetch("unused", request)).text();
      if (variant !== "missing_hook") await fixture.native(FreshFixture.EXPECTED);
      if (variant === "duplicate_part") await fixture.native("PRIVATE_CHANGED_PART");
      await fixture.assistant({ parent, completed: true });
      assert.equal((await fixture.finish()).capture, "incomplete", variant);
    } finally { fixture.close(); }
  }
});

test("fresh response ambiguity, unsupported shapes and text bounds remain exact passthrough and incomplete", async () => {
  const frames = [
    { choices: [{ index: 0, delta: { content: ["PRIVATE_UNKNOWN_CONTENT"] }, finish_reason: "stop" }] },
    { choices: [{ index: 0, delta: { content: FreshFixture.EXPECTED }, finish_reason: "stop" }, { index: 1, delta: { content: "PRIVATE_OTHER_CHOICE" }, finish_reason: "stop" }] },
    { choices: [{ index: 0, delta: { content: "PRIVATE_BOUND".repeat(6000) }, finish_reason: "stop" }] },
    { choices: [{ index: 0, delta: { content: FreshFixture.EXPECTED }, finish_reason: "length" }] },
  ];
  for (const frame of frames) {
    const body = `data: ${JSON.stringify(frame)}\n\ndata: [DONE]\n\n`;
    const fixture = new FreshFixture(() => new Response(body, { headers: { "content-type": "text/event-stream" } }));
    try {
      const fetch = await fixture.install();
      await fixture.root(); await fixture.assistant();
      assert.equal(await (await fetch("unused", fixture.request())).text(), body);
      await fixture.native(FreshFixture.EXPECTED); await fixture.assistant({ completed: true });
      assert.equal((await fixture.finish()).capture, "incomplete");
    } finally { fixture.close(); }
  }
});

test("cancelled fresh response and observer write failure preserve cancellation and original bytes", async () => {
  let cancelled;
  const fixture = new FreshFixture(() => new Response(new ReadableStream({
    pull() {}, cancel(reason) { cancelled = reason; },
  }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } }));
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    const response = await fetch("unused", fixture.request());
    const reader = response.body.getReader(); const pending = reader.read();
    const reason = { value: "PRIVATE_CANCELLATION_REASON" };
    await reader.cancel(reason);
    assert.deepEqual(await pending, { done: true, value: undefined });
    assert.equal(cancelled, reason);
    await fixture.native(FreshFixture.EXPECTED); await fixture.assistant({ completed: true });
    assert.equal((await fixture.finish()).capture, "incomplete");
    fixture.close();
    await fixture.hooks["experimental.text.complete"]({}, { text: "PRIVATE_UNOWNED_TEXT" });
  } finally { fixture.close(); }
});

test("unknown fresh frames and malformed usage cannot hide behind a later valid stop", async () => {
  const unknown = [
    { unexpected: "PRIVATE_UNKNOWN_FRAME" },
    { choices: [] },
    { choices: [], usage: { prompt_tokens: "PRIVATE_BAD_COUNT", completion_tokens: 1, total_tokens: 2 } },
    { choices: [], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2, unknown: "PRIVATE_UNKNOWN_USAGE" } },
  ];
  for (const first of unknown) {
    const body = `data: ${JSON.stringify(first)}\n\ndata: ${JSON.stringify({ choices: [{ index: 0, delta: { content: FreshFixture.EXPECTED }, finish_reason: "stop" }] })}\n\ndata: [DONE]\n\n`;
    const fixture = new FreshFixture(() => new Response(body, { headers: { "content-type": "text/event-stream" } }));
    try {
      const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
      assert.equal(await (await fetch("unused", fixture.request())).text(), body);
      await fixture.native(FreshFixture.EXPECTED); await fixture.assistant({ completed: true });
      assert.equal((await fixture.finish()).capture, "incomplete");
    } finally { fixture.close(); }
  }
});

test("malformed UTF-8 cannot fabricate complete fresh text and bytes remain unchanged", async () => {
  const prefix = new TextEncoder().encode('data: {"choices":[{"index":0,"delta":{"content":"PRIVATE_');
  const suffix = new TextEncoder().encode('"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n');
  const bytes = new Uint8Array([...prefix, 0xff, ...suffix]);
  const fixture = new FreshFixture(() => new Response(CaptureFixture.stream(bytes, 1), { headers: { "content-type": "text/event-stream" } }));
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    assert.deepEqual(new Uint8Array(await (await fetch("unused", fixture.request())).arrayBuffer()), bytes);
    await fixture.native("PRIVATE_�"); await fixture.assistant({ completed: true });
    assert.equal((await fixture.finish()).capture, "incomplete");
  } finally { fixture.close(); }
});

test("fresh content after stop or DONE is not accepted as complete text", async () => {
  for (const afterDone of [false, true]) {
    const terminal = { choices: [{ index: 0, delta: { content: FreshFixture.EXPECTED }, finish_reason: "stop" }] };
    const late = { choices: [{ index: 0, delta: { content: "PRIVATE_LATE_TEXT" }, finish_reason: null }] };
    const body = `data: ${JSON.stringify(terminal)}\n\n${afterDone ? "data: [DONE]\n\n" : ""}data: ${JSON.stringify(late)}\n\ndata: [DONE]\n\n`;
    const fixture = new FreshFixture(() => new Response(body, { headers: { "content-type": "text/event-stream" } }));
    try {
      const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
      assert.equal(await (await fetch("unused", fixture.request())).text(), body);
      await fixture.native(FreshFixture.EXPECTED + "PRIVATE_LATE_TEXT"); await fixture.assistant({ completed: true });
      assert.equal((await fixture.finish()).capture, "incomplete");
    } finally { fixture.close(); }
  }
});

test("known usage-only fresh frames and fragmented Unicode remain complete", async () => {
  const text = "PRIVATE_é🤖";
  const terminal = { choices: [{ index: 0, delta: { content: text }, finish_reason: "stop" }] };
  const usage = { choices: [], usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8, prompt_tokens_details: { cached_tokens: 1 } } };
  const body = `data: ${JSON.stringify(terminal)}\n\ndata: ${JSON.stringify(usage)}\n\ndata: [DONE]\n\n`;
  const bytes = new TextEncoder().encode(body);
  const fixture = new FreshFixture(() => new Response(CaptureFixture.stream(bytes, 1), { headers: { "content-type": "text/event-stream" } }));
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    assert.deepEqual(new Uint8Array(await (await fetch("unused", fixture.request())).arrayBuffer()), bytes);
    await fixture.native(text); await fixture.assistant({ completed: true });
    const summary = await fixture.finish();
    assert.equal(summary.capture, "complete");
    assert.equal(summary.native_equals_provider, true);
    assert.equal(summary.native_text_bytes, Buffer.byteLength(text));
  } finally { fixture.close(); }
});

class ResponsesFixture extends CaptureFixture {
  static EXPECTED = "PRIVATE_FRESH_TOKEN";
  static PROMPT = "Do not call tools. Return only this exact token: PRIVATE_FRESH_TOKEN";

  constructor(frames = ResponsesFixture.frames(), options = {}) {
    const bytes = options.bytes ?? ResponsesFixture.bytes(frames);
    super(options.delegate ?? (() => new Response(CaptureFixture.stream(bytes, options.width ?? 7), {
      status: options.status ?? 200,
      headers: { "content-type": options.contentType ?? "text/event-stream" },
    })));
    this.bytes = bytes;
    this.config = { provider: { openai: {
      npm: "@ai-sdk/openai", models: { PRIVATE_MODEL: { name: "Expected Model" } },
      options: { fetch: this.config.provider.ollama.options.fetch, apiKey: "PRIVATE_KEY" },
    } } };
  }

  async install() {
    this.hooks = await plugin({}, {
      outputDirectory: this.directory, expectedProvider: "openai", expectedModelName: "Expected Model",
      freshOutput: { prompt: ResponsesFixture.PROMPT, expected: ResponsesFixture.EXPECTED },
    });
    await this.hooks.config(this.config);
    return this.config.provider.openai.options.fetch;
  }

  request() {
    return { method: "POST", headers: { authorization: "PRIVATE_AUTH", "x-opencode-session-id": "PRIVATE_SESSION" },
      body: JSON.stringify({ model: "PRIVATE_MODEL", stream: true,
        input: [{ role: "developer", content: "PRIVATE_CONTEXT" },
          { role: "user", content: [{ type: "input_text", text: ResponsesFixture.PROMPT }] }],
      }) };
  }

  async root() {
    const input = { sessionID: "PRIVATE_SESSION", messageID: "PRIVATE_USER", model: { providerID: "openai", modelID: "PRIVATE_MODEL" } };
    const output = { message: { id: "PRIVATE_USER", sessionID: "PRIVATE_SESSION", role: "user", agent: "build", model: { ...input.model } },
      parts: [{ id: "PRIVATE_USER_PART", sessionID: "PRIVATE_SESSION", messageID: "PRIVATE_USER", type: "text", text: ResponsesFixture.PROMPT }] };
    const original = JSON.stringify({ input, output });
    await this.hooks["chat.message"](input, output);
    assert.equal(JSON.stringify({ input, output }), original);
  }

  async assistant(completed = false) {
    await this.hooks.event({ event: { type: "message.updated", properties: { info: {
      id: "PRIVATE_ASSISTANT", sessionID: "PRIVATE_SESSION", parentID: "PRIVATE_USER", role: "assistant",
      providerID: "openai", modelID: "PRIVATE_MODEL", time: completed ? { created: 1, completed: 2 } : { created: 1 },
      ...(completed ? { finish: "stop" } : {}),
    } } } });
  }

  async native(text) {
    const output = { text };
    await this.hooks["experimental.text.complete"]({ sessionID: "PRIVATE_SESSION", messageID: "PRIVATE_ASSISTANT", partID: "PRIVATE_NATIVE_PART" }, output);
    assert.equal(output.text, text);
    await this.assistant(true);
  }

  facts() {
    return readdirSync(this.directory).filter((name) => name.endsWith(".jsonl")).flatMap((name) => {
      const path = join(this.directory, name);
      assert.equal(statSync(path).mode & 0o777, 0o600);
      const text = readFileSync(path, "utf8");
      assert(!text.includes("PRIVATE_"), "redacted receipts must contain no raw values");
      return text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
    });
  }

  async finish() {
    await this.hooks.dispose();
    return this.facts().findLast((fact) => fact.event === "fresh_summary");
  }

  async collect(native = ResponsesFixture.EXPECTED) {
    const fetch = await this.install(); await this.root(); await this.assistant();
    assert.deepEqual(new Uint8Array(await (await fetch("PRIVATE_URL", this.request())).arrayBuffer()), this.bytes);
    await this.native(native);
    return this.finish();
  }

  static bytes(frames) {
    return new TextEncoder().encode(frames.map((frame) => `event: ${frame.type}\r\ndata: ${JSON.stringify(frame)}\r\n\r\n`).join(""));
  }

  static frames(parts = [{ delta: ResponsesFixture.EXPECTED }], model = "PRIVATE_MODEL") {
    const response = { id: "PRIVATE_RESPONSE", model, status: "in_progress", output: [], error: null, incomplete_details: null };
    const message = { id: "PRIVATE_MESSAGE", type: "message", role: "assistant", status: "in_progress", content: [] };
    const frames = [{ type: "response.created", response }, { type: "response.in_progress", response },
      { type: "response.output_item.added", output_index: 0, item: message }];
    const itemContent = [];
    const finalContent = [];
    for (const [content_index, values] of parts.entries()) {
      const owner = { output_index: 0, content_index, item_id: message.id };
      frames.push({ type: "response.content_part.added", ...owner, part: { type: "output_text", text: "" } },
        { type: "response.output_text.delta", ...owner, delta: values.delta },
        { type: "response.output_text.done", ...owner, text: values.done ?? values.delta },
        { type: "response.content_part.done", ...owner, part: { type: "output_text", text: values.part ?? values.done ?? values.delta } });
      itemContent.push({ type: "output_text", text: values.item ?? values.part ?? values.done ?? values.delta });
      finalContent.push({ type: "output_text", text: values.final ?? values.item ?? values.part ?? values.done ?? values.delta });
    }
    frames.push({ type: "response.output_item.done", output_index: 0, item: { ...message, status: "completed", content: itemContent } },
      { type: "response.completed", response: { ...response, status: "completed", output: [{ ...message, status: "completed", content: finalContent }] } });
    return frames.map((frame, sequence_number) => ({ ...frame, sequence_number }));
  }
}

test("OpenAI Responses: per-part redistribution remains visible when aggregates match", async () => {
  const fixture = new ResponsesFixture(ResponsesFixture.frames([{ delta: "AB", done: "A" }, { delta: "C", done: "BC" }]));
  try {
    const summary = await fixture.collect("ABC");
    assert.equal(summary.capture, "complete");
    assert.equal(summary.provider_delta_equals_done, true);
    assert.equal(summary.provider_parts_delta_equal_done, false, "equal concatenations must not hide changed individual parts");
    const parts = fixture.facts().filter((fact) => fact.event === "fresh_provider_part");
    assert.equal(parts.length, 2);
    assert.deepEqual(parts.map((part) => part.delta_equals_done), [false, false]);
  } finally { fixture.close(); }
});

test("OpenAI Responses: transient receipt write failure cannot certify retained evidence", async () => {
  const fixture = new ResponsesFixture();
  try {
    const fetch = await fixture.install();
    const receipt = join(fixture.directory, readdirSync(fixture.directory).find((name) => name.endsWith(".jsonl")));
    const held = `${receipt}.held`;
    renameSync(receipt, held); mkdirSync(receipt);
    try { await fixture.root(); } finally { rmSync(receipt, { recursive: true }); renameSync(held, receipt); }
    await fixture.assistant();
    assert.deepEqual(new Uint8Array(await (await fetch("PRIVATE_URL", fixture.request())).arrayBuffer()), fixture.bytes);
    await fixture.native(ResponsesFixture.EXPECTED);
    const summary = await fixture.finish();
    assert.equal(summary.capture, "incomplete", "a later complete summary must not erase missing retained provenance");
    assert.equal(summary.receipt_persistence_complete, false);
  } finally { fixture.close(); }
});

test("OpenAI Responses: fragmented Unicode preserves request receiver metadata and exact bytes", async () => {
  const frames = ResponsesFixture.frames([{ delta: "PRIVATE_é🤖" }]);
  const bytes = ResponsesFixture.bytes(frames);
  const receiver = {};
  let request;
  const fixture = new ResponsesFixture(frames, { delegate: function (...args) {
    assert.equal(this, receiver); assert.equal(args[0], "PRIVATE_URL"); assert.equal(args[1], request);
    const response = new Response(CaptureFixture.stream(bytes, 1), { status: 201, statusText: "Created", headers: { "content-type": "text/event-stream", "x-private": "PRIVATE_HEADER" } });
    Object.defineProperty(response, "url", { value: "PRIVATE_RESPONSE_URL" });
    Object.defineProperty(response, "redirected", { value: true });
    return response;
  } });
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    request = fixture.request(); const original = JSON.stringify(request);
    const response = await Reflect.apply(fetch, receiver, ["PRIVATE_URL", request]);
    assert.equal(response.status, 201); assert.equal(response.statusText, "Created");
    assert.equal(response.url, "PRIVATE_RESPONSE_URL"); assert.equal(response.redirected, true); assert.equal(response.type, "default");
    assert.equal(response.headers.get("x-private"), "PRIVATE_HEADER");
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), bytes); assert.equal(JSON.stringify(request), original);
    await fixture.native("PRIVATE_é🤖");
    const comparison = fixture.facts().findLast((fact) => fact.event === "fresh_comparison");
    assert.equal(comparison.capture, "complete");
    assert.equal(comparison.native_equals_provider_final, true);
    assert.equal(comparison.provider_delta_sha256, createHash("sha256").update("PRIVATE_é🤖").digest("hex"));
    assert.equal(comparison.native_text_sha256, comparison.provider_delta_sha256);
    assert.equal(comparison.native_session_sha256, createHash("sha256").update("PRIVATE_SESSION").digest("hex"));
    assert.equal(comparison.native_user_sha256, createHash("sha256").update("PRIVATE_USER").digest("hex"));
    assert.equal(comparison.native_assistant_sha256, createHash("sha256").update("PRIVATE_ASSISTANT").digest("hex"));
    assert.deepEqual(comparison.native_parts, [{ part_sha256: createHash("sha256").update("PRIVATE_NATIVE_PART").digest("hex"),
      text_sha256: comparison.native_text_sha256, text_bytes: Buffer.byteLength("PRIVATE_é🤖") }]);
    assert.equal((await fixture.finish()).receipt_persistence_complete, true);
  } finally { fixture.close(); }
});

test("OpenAI Responses: snapshot disagreements remain complete independent observations", async () => {
  const fields = ["delta", "done", "part", "item", "final", "native"];
  for (const changed of fields) {
    const values = Object.fromEntries(fields.map((field) => [field, field === changed ? "PRIVATE_DIFFERENT" : ResponsesFixture.EXPECTED]));
    const fixture = new ResponsesFixture(ResponsesFixture.frames([values]));
    try {
      const summary = await fixture.collect(values.native);
      assert.equal(summary.capture, "complete", changed);
      assert.equal(summary.provider_delta_equals_expected, changed !== "delta", changed);
      assert.equal(summary.provider_done_equals_expected, changed !== "done", changed);
      assert.equal(summary.provider_final_equals_expected, changed !== "final", changed);
      assert.equal(summary.native_equals_expected, changed !== "native", changed);
      assert.equal(summary.native_equals_provider_final, values.native === values.final, changed);
      const part = fixture.facts().find((fact) => fact.event === "fresh_provider_part");
      assert.equal(part.delta_equals_done, values.delta === values.done, changed);
      assert.equal(part.done_equals_content, values.done === values.part, changed);
      assert.equal(part.content_equals_item, values.part === values.item, changed);
      assert.equal(part.item_equals_final, values.item === values.final, changed);
      for (const field of ["delta", "done", "final"]) {
        assert.equal(summary[`provider_${field}_sha256`], createHash("sha256").update(values[field]).digest("hex"));
      }
    } finally { fixture.close(); }
  }
});

test("OpenAI Responses: correlation rejects cross-response and cross-part attribution", async () => {
  for (const variant of ["response", "model_changed", "item", "output_index", "content_index", "duplicate_item", "final_inventory"]) {
    const frames = ResponsesFixture.frames([{ delta: "A" }, { delta: "B" }]);
    if (variant === "response") frames.at(-1).response.id = "PRIVATE_OTHER_RESPONSE";
    if (variant === "model_changed") frames.at(-1).response.model = "PRIVATE_OTHER_MODEL";
    if (variant === "item") frames.find((frame) => frame.type === "response.output_text.delta").item_id = "PRIVATE_OTHER_MESSAGE";
    if (variant === "output_index") frames.find((frame) => frame.type === "response.output_text.delta").output_index = 1;
    if (variant === "content_index") frames.find((frame) => frame.type === "response.output_text.delta").content_index = 1;
    if (variant === "duplicate_item") frames.splice(3, 0, { ...frames[2], output_index: 1 });
    if (variant === "final_inventory") frames.at(-1).response.output.push({ ...frames.at(-1).response.output[0], id: "PRIVATE_EXTRA_MESSAGE" });
    const fixture = new ResponsesFixture(frames.map((frame, sequence_number) => ({ ...frame, sequence_number })));
    try {
      assert.equal((await fixture.collect("AB")).capture, "incomplete", variant);
      assert.equal(fixture.facts().find((fact) => fact.event === "fresh_provider_text").capture, "incomplete", variant);
    } finally { fixture.close(); }
  }
});

test("OpenAI Responses: request identity and reported model have distinct attribution facts", async () => {
  for (const variant of ["model", "session", "stream", "extra_text", "reported_model"]) {
    const fixture = new ResponsesFixture(ResponsesFixture.frames(undefined, variant === "reported_model" ? "PRIVATE_REPORTED_ALIAS" : "PRIVATE_MODEL"));
    try {
      const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
      const request = fixture.request(); const body = JSON.parse(request.body);
      if (variant === "model") body.model = "PRIVATE_WRONG_MODEL";
      if (variant === "session") request.headers["x-opencode-session-id"] = "PRIVATE_WRONG_SESSION";
      if (variant === "stream") body.stream = false;
      if (variant === "extra_text") body.input.at(-1).content.push({ type: "input_text", text: "PRIVATE_NATIVE_REMINDER" });
      request.body = JSON.stringify(body); const original = JSON.stringify(request);
      assert.deepEqual(new Uint8Array(await (await fetch("PRIVATE_URL", request)).arrayBuffer()), fixture.bytes);
      assert.equal(JSON.stringify(request), original);
      await fixture.native(ResponsesFixture.EXPECTED);
      const summary = await fixture.finish();
      assert.equal(summary.capture, ["extra_text", "reported_model"].includes(variant) ? "complete" : "incomplete", variant);
      if (variant === "reported_model") assert.equal(summary.provider_reported_model_matches_requested, false);
      if (variant === "extra_text") {
        const facts = fixture.facts().find((fact) => fact.event === "fresh_request");
        assert.equal(facts.exact_prompt_parts, 1); assert.equal(facts.other_user_text_parts, 1);
      }
    } finally { fixture.close(); }
  }
});

test("OpenAI Responses: missing duplicate late and truncated events stay inconclusive", async () => {
  for (const variant of ["missing_created", "missing_done", "missing_completed", "sequence_gap", "duplicate_sequence", "late_delta", "duplicate_completed", "trailing", "unknown_event", "sentinel"]) {
    let frames = ResponsesFixture.frames();
    if (variant === "missing_created") frames.shift();
    if (variant === "missing_done") frames = frames.filter((frame) => frame.type !== "response.output_text.done");
    if (variant === "missing_completed") frames.pop();
    if (variant === "late_delta") frames.splice(-2, 0, frames.find((frame) => frame.type === "response.output_text.delta"));
    if (variant === "duplicate_completed") frames.push(frames.at(-1));
    if (variant === "unknown_event") frames.splice(2, 0, { type: "response.private_unknown" });
    frames = frames.map((frame, sequence_number) => ({ ...frame, sequence_number }));
    if (variant === "sequence_gap") frames[2].sequence_number++;
    if (variant === "duplicate_sequence") frames[2].sequence_number--;
    const suffix = variant === "trailing" ? "data: {" : variant === "sentinel" ? "data: [DONE]\n\n" : "";
    const fixture = new ResponsesFixture(frames, { bytes: new Uint8Array([...ResponsesFixture.bytes(frames), ...new TextEncoder().encode(suffix)]) });
    try { assert.equal((await fixture.collect()).capture, "incomplete", variant); } finally { fixture.close(); }
  }
});

test("OpenAI Responses: supported reasoning items preserve ordered text attribution", async () => {
  const frames = ResponsesFixture.frames();
  for (const frame of frames) if (Object.hasOwn(frame, "output_index")) frame.output_index = 1;
  const reasoning = { id: "PRIVATE_REASONING", type: "reasoning", status: "completed", summary: [] };
  frames.splice(2, 0,
    { type: "response.output_item.added", output_index: 0, item: { ...reasoning, status: "in_progress" } },
    { type: "response.output_item.done", output_index: 0, item: reasoning });
  frames.at(-1).response.output.unshift(reasoning);
  const fixture = new ResponsesFixture(frames.map((frame, sequence_number) => ({ ...frame, sequence_number })));
  try {
    assert.equal((await fixture.collect()).capture, "complete");
    assert.equal(fixture.facts().find((fact) => fact.event === "fresh_provider_part").output_index, 1);
  } finally { fixture.close(); }
});

test("OpenAI Responses: unsupported shapes encoding and bounds preserve every forwarded byte", async () => {
  for (const variant of ["nontext", "malformed_json", "malformed_utf8", "frame_bound", "text_bound", "response_bound", "part_bound", "status", "content_type"]) {
    let frames = ResponsesFixture.frames();
    if (variant === "nontext") frames[2].item.type = "function_call";
    if (variant === "text_bound") frames = ResponsesFixture.frames([{ delta: "X".repeat(65 << 10) }]);
    if (variant === "part_bound") frames = ResponsesFixture.frames(Array.from({ length: 17 }, () => ({ delta: "X" })));
    let bytes = ResponsesFixture.bytes(frames);
    if (variant === "malformed_json") bytes = new TextEncoder().encode("data: {PRIVATE_MALFORMED}\n\n");
    if (variant === "malformed_utf8") bytes = new Uint8Array([...bytes, 0xff, 10, 10]);
    if (variant === "frame_bound") bytes = new TextEncoder().encode(`data: ${"X".repeat(257 << 10)}\n\n`);
    if (variant === "response_bound") bytes = new Uint8Array([...bytes, ...new TextEncoder().encode(": padding\n\n".repeat(900000))]);
    const fixture = new ResponsesFixture(frames, { bytes, width: 65536,
      status: variant === "status" ? 500 : 200, contentType: variant === "content_type" ? "application/json" : "text/event-stream" });
    try {
      assert.equal((await fixture.collect()).capture, "incomplete", variant);
      if (variant === "response_bound") {
        const directory = join(fixture.directory, "private-pending");
        const manifest = JSON.parse(readFileSync(join(directory, readdirSync(directory).find((name) => name.endsWith(".manifest.json"))), "utf8"));
        assert.equal(manifest.body_capture_complete, false); assert.equal(manifest.response_bound_exceeded, true);
        assert.equal(manifest.captured_response_bytes, 8 << 20);
        assert.deepEqual(new Uint8Array(readFileSync(join(directory, manifest.response_file))), bytes.subarray(0, 8 << 20));
      }
    } finally { fixture.close(); }
  }
});

test("OpenAI Responses: cancellation is lazy and settles the outstanding native read", async () => {
  let pulls = 0; let cancelled;
  const fixture = new ResponsesFixture(undefined, { delegate: () => new Response(new ReadableStream({
    pull() { pulls++; }, cancel(reason) { cancelled = reason; },
  }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } }) });
  try {
    const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
    const response = await fetch("PRIVATE_URL", fixture.request()); assert.equal(pulls, 0);
    const reader = response.body.getReader(); const pending = reader.read(); await Promise.resolve();
    assert.equal(pulls, 1);
    const reason = { private: "PRIVATE_CANCEL_REASON" }; await reader.cancel(reason);
    assert.deepEqual(await pending, { done: true, value: undefined }); assert.equal(cancelled, reason);
    await fixture.native(ResponsesFixture.EXPECTED);
    assert.equal((await fixture.finish()).capture, "incomplete");
  } finally { fixture.close(); }
});

test("OpenAI Responses: native fetch and body failures retain identity despite receipt failure", async () => {
  for (const phase of ["fetch", "body"]) {
    const failure = new Error("PRIVATE_NATIVE_FAILURE");
    const fixture = new ResponsesFixture(undefined, { delegate: () => {
      if (phase === "fetch") throw failure;
      return new Response(new ReadableStream({ pull(controller) { controller.error(failure); } }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });
    } });
    try {
      const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
      fixture.close();
      await assert.rejects(async () => (await fetch("PRIVATE_URL", fixture.request())).text(), (actual) => actual === failure);
    } finally { fixture.close(); }
  }
});

test("OpenAI Responses: private staging keeps exact bounded bodies separate from redacted receipts", async () => {
  const fixture = new ResponsesFixture();
  try {
    assert.equal((await fixture.collect()).capture, "complete");
    const directory = join(fixture.directory, "private-pending");
    assert.equal(statSync(directory).mode & 0o777, 0o700);
    const files = readdirSync(directory); assert.equal(files.length, 3);
    for (const file of files) assert.equal(statSync(join(directory, file)).mode & 0o777, 0o600);
    const manifest = JSON.parse(readFileSync(join(directory, files.find((name) => name.endsWith(".manifest.json"))), "utf8"));
    assert.equal(manifest.protocol, "openai_responses"); assert.equal(manifest.body_capture_complete, true);
    assert.equal(manifest.provider_correlation_complete, true); assert.equal(manifest.receipt_persistence_complete, true);
    assert.equal(manifest.eof, true); assert.equal(manifest.response_bound_exceeded, false);
    const request = readFileSync(join(directory, manifest.request_file), "utf8");
    const response = readFileSync(join(directory, manifest.response_file));
    assert.equal(request, fixture.request().body); assert.deepEqual(new Uint8Array(response), fixture.bytes);
    assert.equal(manifest.request_bytes, Buffer.byteLength(request)); assert.equal(manifest.response_bytes, response.length);
    assert.equal(manifest.captured_response_bytes, response.length);
    for (const secret of ["PRIVATE_AUTH", "PRIVATE_KEY", "PRIVATE_URL"]) {
      assert(!request.includes(secret)); assert(!response.includes(secret)); assert(!JSON.stringify(manifest).includes(secret));
    }
    fixture.facts();
  } finally { fixture.close(); }
});

test("OpenAI Responses: private staging failures and oversized requests remain inconclusive", async () => {
  for (const variant of ["directory_mode", "request_bound"]) {
    const fixture = new ResponsesFixture();
    try {
      const fetch = await fixture.install(); await fixture.root(); await fixture.assistant();
      if (variant === "directory_mode") mkdirSync(join(fixture.directory, "private-pending"), { mode: 0o755 });
      const request = fixture.request();
      if (variant === "request_bound") {
        const value = JSON.parse(request.body); value.input[0].content = "X".repeat(2 << 20); request.body = JSON.stringify(value);
      }
      assert.deepEqual(new Uint8Array(await (await fetch("PRIVATE_URL", request)).arrayBuffer()), fixture.bytes);
      await fixture.native(ResponsesFixture.EXPECTED);
      assert.equal((await fixture.finish()).capture, "incomplete", variant);
      if (variant === "directory_mode") assert.deepEqual(readdirSync(join(fixture.directory, "private-pending")), [], "invalid directory permissions must prevent every private body write");
    } finally { fixture.close(); }
  }
});
