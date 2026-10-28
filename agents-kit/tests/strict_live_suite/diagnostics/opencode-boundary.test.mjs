import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from "node:fs";
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

  async install() {
    this.hooks = await plugin({}, {
      outputDirectory: this.directory, expectedModelName: "Expected Model",
      freshOutput: { prompt: FreshFixture.PROMPT, expected: FreshFixture.EXPECTED },
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
      message: { id, sessionID: "PRIVATE_SESSION", role: "user", model: { ...input.model } },
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
