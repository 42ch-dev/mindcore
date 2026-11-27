# A native addon needs one failure channel and declarations written against the wire

**Problem.** Two things decide whether a JavaScript caller can use a Rust addon: how failures cross
the boundary, and whether the `.d.ts` describes the objects that actually arrive. Both are easy to
get subtly wrong in ways no Rust test sees.

**One failure channel.** An exported `async fn` in napi-rs rejects only with `napi::Error`, which
carries a message and neither the facade's error code nor its structured details. Two channels —
a thrown `Error` for some paths and a result object for others — force every caller (and the one
JS wrapper that turns results into values or errors) to handle both shapes. Answer **one** result
object for every asynchronous export:

```text
{ ok: true, value }                     the wrapper resolves with value
{ ok: false, code, message, details? }  the wrapper rejects with a typed error
```

Keep a code reserved for the binding's own defects (`BINDING_DEFECT` for a value that cannot be
serialized), so a caller can never mistake a binding bug for a legitimate refusal. A handle cannot
travel inside `serde_json::Value`; a handle-returning export builds the same object with the handle
in `value`.

**Declarations must name the fields, not `object`.** Aliasing every wire type to `object` type-checks
and is useless: `capture(...).result.candidate_ids` fails to compile, so a TypeScript caller cannot
read the data the flow exists to produce. Derive each declaration from the Rust struct (`serde`
renames, `skip_serializing_if` → optional), and say in a comment which struct it mirrors. Verify with
a strict `tsc` run over a consumer that reads every field the flow needs, plus a negative control
that must fail — otherwise the declarations can silently be `any` in disguise.

**Handle construction is factory-only.** Declare `private constructor(handle: object)` on wrapper
classes; without a declared constructor TypeScript accepts `new MindcoreClient()` while the JS
constructor requires a native handle, so the type system advertises a call that always throws.

**Evidence.** `crates/mindcore-binding/src/lib.rs`, `bindings/node/index.d.ts`,
`bindings/node/index.js`; tests `an_answer_that_does_not_serialize_is_a_binding_defect` and the
`node --test` suite in `bindings/node/test.mjs`.
