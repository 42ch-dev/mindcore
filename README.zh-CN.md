# mindcore

[![CI](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml/badge.svg)](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust: stable](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![crates.io](https://img.shields.io/crates/v/mindcore-base.svg)](https://crates.io/crates/mindcore-base)
[![Status: alpha](https://img.shields.io/badge/status-alpha-yellow.svg)](#状态与边界)

**一个 Mind 的记忆、知识与认知，存进一个嵌入式存储，用普通调用读写。**

Mindcore 是一个 Rust 库，供需要 Mind 的应用使用：agent **听到**了什么、它**采纳**了什么作为
自己的知识、以及它当前**判定**什么是真的，三者分开保存，各有一条记录，连接它们的 provenance
在每次修订后仍然成立。

[English](README.md) | **简体中文**

## 什么是 Mind

一个 Mind 就是一个记忆空间。它的 id 就是这个空间的 id——`mindcore-sqlite` 只维护一份 registry，
Mind 没有第二个身份，因此 registry 行、空间与 cognition carrier 不会互相矛盾。
`mindcore_runtime::minds` 是进程内入口：`create` 在一个事务里写入空间、cognition carrier、
registry 行与 owner 的默认授权行，被拒绝时不留下任何行；`list` 与 `get` 按调用方当前有效的授权
把 registry 读回来。

| 记录 | 形态 | 回答的问题 |
| --- | --- | --- |
| Memory | candidate → retained record | 它听到了、观察到了什么？ |
| Knowledge | 带修订号的 scoped `KnowledgeEntry` | 它为自己采纳了什么，来自哪里？ |
| Cognition | 追加式 carrier records → `MindState` | 它当前判定什么为真，依据是什么？ |

`mindcore-base` 定义这些记录类型及其不变量，本身不做任何 I/O；`mindcore-sqlite` 是持久化它们的
唯一实现。一条 memory 不会因为被读取就变成个人信念，一条被采纳的 entry 不会因为共享源变化而悄悄
改变，只有显式的 carrier 更新才会改变当前判断。这种分离才是产品本身。

`mindcore_runtime::Mind` 是一个 Mind 之上的句柄：`open` 只做一次 registry 读取，之后每个操作都在
自己的事务上重新读取实时状态——句柄不缓存授权，也不授予授权。`capture`、`recall` 与
`review` 组合 memory adapter；`knowledge_get`、`knowledge_put` 与 `knowledge_list` 组合 scoped
store；`cognition_read` 与 `deliberate` 抵达当前判断及其背后的 workflows。没有任何方法替你选择
collection、owner 或视角，也没有任何方法替你选择 subject。

## 三种接入方式

三种形态通向同一个内核，因此每一个都遵守同一条授权规则：读取任何目标之前，先对 (principal, Mind)
这一对做实时准入。跨进程的契约是 SPOKE Connect，HTTP API 不属于其中任何一种形态。

| 形态 | 适合谁 | 由谁承载 | 当前状态 |
| --- | --- | --- | --- |
| 进程内 Rust | 把库链接进自己进程的 Rust 宿主 | `mindcore-runtime`：`minds::{create, list, get}` 与 `Mind` 句柄 | 已在本仓库构建，`publish = false` |
| 进程内 TypeScript | 把 addon 链接进自己进程的 Node 宿主 | `crates/mindcore-binding`（napi `cdylib`），由 `bindings/node` 包加载（`MindcoreClient`、`MindView`、`MindcoreError`） | 从本仓库构建；该 package 为 `private`，不在 npm 上 |
| SPOKE 对端 | 任何 SPOKE Connect peer | `mindcore-daemon` 通过 Unix socket 提供一个；自行组装 `MindcoreServe` 的宿主无需 daemon 也能服务对端 | 已在本仓库构建，`publish = false` |

binding 在自己的进程内调用 facade，而不是去连另一个进程；而一个连接 daemon 的 TypeScript 宿主用的
是 SPOKE 对端形态，不是 TypeScript 形态。三种形态都通过 `mindcore-sqlite` 读写同一个 SQLite 存储。

## 快速开始

```toml
[dependencies]
# facade 尚未发布，因此整个 mindcore family 取自本仓库的同一个 revision：
# 把 registry 的 `mindcore-base` 与 git 的 `mindcore-runtime` 混用会得到同一套类型的第二份副本，
# facade 不会接受它。
mindcore-base = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-sqlite = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-runtime = { git = "https://github.com/42ch-dev/mindcore" }
spoke-operations = "0.19.1"
spoke-schemas = "0.19.1"
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "sqlite"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
```

`mindcore-base`、`mindcore-spi` 与 `mindcore-sqlite` 在 crates.io 上是 `0.1.0`，与这个 workspace
声明的版本一致，因此 `"0.1"` 要求可以匹配；只需要记录模型与 store 的宿主可以直接从 registry 引入。
facade 为 `publish = false`，因此这个示例把整个 family 从本仓库的同一个 revision 引入——把 registry
的 `mindcore-base` 与 git 的 `mindcore-runtime` 混用会得到同一套类型的两份副本。

```rust
use mindcore_base::boundary::CallerId;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{Capture, Mind};
use mindcore_sqlite::MindStore;
use serde_json::json;
use spoke_operations::SpokeResult;

/// facade 用拒绝值而不是 panic 回答；宿主把它映射出来并保留 code。
fn ok<T>(result: SpokeResult<T>) -> Result<T, Box<dyn std::error::Error>> {
    match result {
        SpokeResult::Ok(value) => Ok(value),
        SpokeResult::Reject(reject) => Err(format!("{}: {}", reject.code, reject.message).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MindStore::open(std::path::Path::new("mind.db")).await?;
    let caller = CallerId::new("caller_wren");

    let row = ok(minds::create(&store, &CreateMind {
        mind_id: None,
        display_name: Some("Wren".to_string()),
        owner: caller.clone(),
    }).await)?;
    let mind = ok(Mind::open(&store, &caller, &row.id).await)?;

    // 宿主先写入 memory 写操作所需的 subject 行；`capture` 对未知 subject 回答 NOT_FOUND。
    sqlx::query(
        "INSERT INTO memory_subjects (space_id, subject_id, status, revision, created_at) \
         VALUES (?, ?, 'active', 1, '2026-01-01T00:00:00.000Z')",
    )
    .bind(row.id.as_str())
    .bind("wren")
    .execute(&store.pool())
    .await?;

    ok(mind.capture(&Capture {
        subject: "wren",
        context: None,
        content: &json!({ "text": "the north gate was closed at dusk" }),
        occurred_at: None,
        kind_hint: None,
        idempotency_key: "capture-north-gate",
    }).await)?;

    Ok(())
}
```

facade 路径——创建 Mind、capture、review、recall、写入 carrier 并读取推导出的 judgment，以及在
宿主提供 reasoner 时 deliberate——见 [docs/quickstart.md](docs/quickstart.md)。完整的记录故事——
采纳、怀疑、证据、共享变更、重新打开以及各种拒绝——由仓库 example 断言：

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

记录模型、修订规则与 provenance 见 [docs/concepts.md](docs/concepts.md)。

## 服务对端

`mindcore-daemon` 是对端形态的参考宿主：一个进程、一个 store 文件、一个 Unix socket。它的 TOML
配置指明 store、socket、responder 的 seed 文件、journal 目录、准入的对端（每个 `public_key` 映射到
一个 `principal`）以及唯一一条 adoption 路径；每张表都拒绝未知键，所以拼错是启动失败，而不是被静默
忽略的设置。它在打开 store 之前先取单实例锁，因此一个数据库文件只由一个进程服务；收到 `SIGTERM`
或 `SIGINT` 时它做 drain，而不是丢掉存活中的 responder。同一 store 上的第二个实例以退出码 2 结束。

```sh
cargo run --locked -p mindcore-daemon -- --config daemon.toml
```

它实际提供的是 `mindcore-runtime` 的表面。`MindcoreServe` 把 workflow engine、路由后的 knowledge
face、宿主的 memory service 以及可选的 history 与 adoption face 组装成 `MindcoreSurface`：构建
`ConnectResponder` 所需的 ports、manifest 与 handlers。对外声明的 `capabilities` 与 `tools` 由实际
提供的能力推导，而不是来自宿主传入的字符串；runtime 也拒绝替宿主臆造只有宿主才能决定的东西——
transport、身份、准入与 journal publisher。提供同样东西的嵌入式宿主自行构建 responder，无需 daemon。

宿主承担的义务、`ServeFaces` 的字段与实际提供的操作清单见 [docs/serving.md](docs/serving.md)。

## 存储与保证

- **单一嵌入式存储。** `mindcore-sqlite` 通过 `MindStore::open` 在单连接池与一个内嵌 migration 上
  打开该 SQLite 文件，启用 WAL、外键与 busy timeout。
- **幂等写入。** 重复的 idempotency key 会重新返回已记录的结果，而不是再次执行；重放读取的是
  receipt，而不是重跑操作。
- **条件修订。** `expected_base_revision` 让每次 knowledge 更新成为 compare-and-swap，拒绝不会改动
  该行。
- **可解析的 provenance。** 被采纳的副本锚定到来源的 scope、entry 与 revision，并沿来源的
  retirement chain 解析，因此它命名的是存活终端，而不是已退休的中间跳。
- **不可变的 migration。** 已发布的 migration 文件永不改变——lock 记录其 sha256——新 schema 以新增
  文件追加。已应用 migration 仍与内嵌集合一致的数据库可以重开；否则会被拒绝，而不是被转换。

`open` 接受与拒绝哪些数据库见[存储兼容性](docs/storage-compatibility.md)；其背后的版本策略见
[SemVer](docs/semver.md)。

## 状态与边界

- **alpha，且大多未发布。** `mindcore-base`、`mindcore-spi` 与 `mindcore-sqlite` 已发布到
  crates.io，registry 上目前是 alpha 版本。`mindcore-spoke`、`mindcore-runtime`、
  `mindcore-daemon` 与 `mindcore-binding` 均为 `publish = false`，由本仓库引入。
- **只到 Mind 层。** facade 与 served surface 只暴露 Mind 层操作。其下的 space 层能力属于尚未暴露
  的高级表面，尚未按 Mind 提供的操作也不会被组合。
- **没有 HTTP 表面。** 本仓库不含任何 HTTP listener、route 或 request type；跨进程契约是
  SPOKE Connect。
- **尚无数据搬迁。** 把既有 store 或 provider 迁入 Mindcore 尚未实现，也没有升级工具。
- **每个数据库文件一个 store。** 每个打开的 `MindStore` 拥有一个连接池，同一文件上开两个 store
  不受支持；daemon 的锁就是单个进程维持这一点的办法。
- **addon 需要构建，不在发布物里。** `bindings/node` 从 `target/{debug,release}` 或
  `MINDCORE_BINDING_PATH` 指定的路径加载 `libmindcore_binding`；构建命令为
  `cargo build -p mindcore-binding`。

## 贡献与变更日志

贡献约定与本地检查见 [CONTRIBUTING.md](CONTRIBUTING.md)，发布历史见 [CHANGELOG.md](CHANGELOG.md)。
在这个仓库里工作（而不是嵌入这个库）见 [docs/for-agents.md](docs/for-agents.md)，发布步骤见
[docs/releasing.md](docs/releasing.md)。
