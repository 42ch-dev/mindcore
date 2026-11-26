# mindcore

[![CI](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml/badge.svg)](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust: stable](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![crates.io](https://img.shields.io/crates/v/mindcore-base.svg)](https://crates.io/crates/mindcore-base)
[![Status: alpha](https://img.shields.io/badge/status-alpha-yellow.svg)](#状态与边界)

**记忆、知识与认知的沉淀层。**

Mindcore 是一个可嵌入的 Rust 库，把三件事分开保存：agent **听到**了什么、它**采纳**了什么作为
个人知识、以及它当前**判定**什么是真的。三者各有一条记录，连接它们的 provenance 在每次修订后
仍然成立。

这里不提供 server、UI 或 transport。宿主嵌入存储，并自行决定暴露什么接口；`mindcore-runtime`
只把构建 SPOKE Connect responder 所需的部件组装起来。

> [!NOTE]
> `mindcore-base`、`mindcore-spi` 与 `mindcore-sqlite` 已发布到 crates.io，registry 上目前是
> alpha 版本。`mindcore-spoke` 与 `mindcore-runtime` 保持私有，由本仓库引入。migration 文件一经
> 发布即不可变，migration lock 为打 tag 把关；`open` 接受与拒绝哪些数据库，见
> [存储兼容性](docs/storage-compatibility.md)。

[English](README.md) | **简体中文**

## 为什么叫 "settlement"

| 层 | 记录 | 回答的问题 |
| --- | --- | --- |
| Memory | candidate → retained record | agent 听到了、观察到了什么？ |
| Knowledge | 带修订号的 scoped `KnowledgeEntry` | 它为自己采纳了什么，来自哪里？ |
| Cognition | carrier records → `MindState` | 它当前判定什么为真，依据是什么？ |

一条 memory 不会因为被读取就变成个人信念；一条被采纳的 entry 不会因为共享源变化而悄悄改变；
只有显式更新 carrier 才会改变当前判断。这种分离才是产品本身。

## 架构

![Mindcore 分层：宿主嵌入 mindcore-runtime，后者组合 SPI workflows、SPOKE adapter 与 SQLite store](docs/assets/architecture.svg)

宿主负责 transport、身份、准入与存储路径；Mindcore 负责记录模型、修订与重放规则、协议适配器，
以及持久化边界。

## 能力概览

- **带重放的 memory 生命周期。** `capture` 暂存 candidate；review 产出 1 条、多条 fragment 或
  0 条记录；每个变更都幂等——重复的 key 会重新返回已记录的结果，而不是再次执行。
- **带条件修订的 scoped knowledge。** 每次写入在 port 构建时就绑定 collection；
  `expected_base_revision` 让每次更新成为 compare-and-swap，拒绝不会改动该行。
- **带 provenance 的采纳。** 被采纳的副本携带指向来源 scope、entry 与 revision 的 anchor。
  anchor 沿来源的 retirement chain 解析，因此副本命名的是存活终端，而不是已退休的中间跳。
- **认知 carrier。** 当前判断是某个 holder entry 下的追加式记录集合，在唯一的持久化边界校验；
  `MindState` 由它派生，不会重复存储。
- **只广告实际提供能力的 served surface。** `MindcoreServe` 从组合出的 faces 与注册项推导
  capabilities 和 tools，不读取宿主提供的字符串。
- **单一嵌入式存储。** SQLite 启用 WAL、外键与 busy timeout；`MindStore::open` 通过单连接池和
  一个内嵌 migration 打开它。

## Crates

| Crate | 职责 | 发布 |
| --- | --- | --- |
| `mindcore-base` | 与存储无关的领域核心：id 与信任边界、memory 协议类型与 IR、knowledge scope 与采纳 provenance、cognition carrier 与 `MindState` 派生。无 I/O。 | [crates.io](https://crates.io/crates/mindcore-base) |
| `mindcore-spi` | MIND ladder 契约，钉在已发布的 SPOKE DTO 上：recall、absorb、deliberate workflows，以及 prompt-reasoner bridge。契约面不含存储 crate、网络或模型 SDK。 | [crates.io](https://crates.io/crates/mindcore-spi) |
| `mindcore-sqlite` | SQLite（SQLx）后端：单连接池、单 migration、WAL 与 busy timeout；在 `MemoryAdapter` 之后实现协议 repository，以及 scoped `KnowledgeEntryPort`，并在唯一持久化边界守护 cognition 模块。 | [crates.io](https://crates.io/crates/mindcore-sqlite) |
| `mindcore-spoke` | 基于借用事务 repository 的 SPOKE memory 协议适配器：wire 校验、wire↔IR 边界、reject 映射、准入、重放与 staging。它从不开启、提交或回滚事务。 | 否 —— 从 git 引入 |
| `mindcore-runtime` | served surface：`MindcoreServe` 把 workflow engine、SQLite knowledge face 与宿主的 memory face 组装成构建 Connect responder 所需的部件，并从实际提供的能力推导广告的 capabilities 与 tools。 | 否 —— 从 git 引入 |

## 快速开始

用 Cargo 添加已发布的 crate；Cargo 解析 registry 上的版本，而本仓库的
`[workspace.package].version` 才是某个 release 包含什么的唯一事实来源：

```sh
cargo add mindcore-base mindcore-sqlite
```

`mindcore-spoke` 与 `mindcore-runtime` 尚未发布，从本仓库引入。SPOKE 的 pin 与本 workspace
声明的一致：

```toml
[dependencies]
# 与本 workspace 的 Cargo.toml 中固定的 SPOKE 版本保持一致
spoke-operations = ">=0.19.1, <0.20.0"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
# 未发布：从本仓库引入
mindcore-spoke = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-runtime = { git = "https://github.com/42ch-dev/mindcore" }
```

打开存储，并通过绑定 collection 的 port 读取：

```rust
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_sqlite::MindStore;
use spoke_operations::{KnowledgeEntryPort, SpokeResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MindStore::open(std::path::Path::new("mind.db")).await?;
    let wren = store.knowledge(KnowledgeScope::new("wren"));

    match wren.get_knowledge_entry("ke_north_gate").await {
        SpokeResult::Ok(entry) => println!("entry at revision {:?}", entry.revision),
        SpokeResult::Reject(reject) => println!("{}: {}", reject.code, reject.message),
    }
    Ok(())
}
```

然后运行完整的 first-mind 故事：听到、采纳、怀疑、证据、共享变更、重新打开与拒绝，每一步都有断言。

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

这个 example 与集成测试运行同一份代码。想要按步骤手写一遍，从
[Quick start](docs/quickstart.md) 开始。

## 文档

| 文档 | 什么时候读 |
| --- | --- |
| [Quick start](docs/quickstart.md) | 想在本地跑通 store、captured memory 与派生出的 `MindState`。 |
| [Concepts](docs/concepts.md) | 在基于它设计之前，需要记录模型、修订规则与 provenance。 |
| [Serving a surface](docs/serving.md) | 正在把 `MindcoreServe` 接入 SPOKE Connect responder。 |
| [Migrating from `tools.mindcore.adopt`](docs/migrating-from-tools-adopt.md) | 把既有宿主迁移到 `knowledge.adopt`，或重新打开旧数据库。 |
| [For agents](docs/for-agents.md) | 你在这个仓库里工作，而不是在嵌入这个库。 |

## 状态与边界

- **仅嵌入式。** `mindcore-runtime` 不开启 listener、transport 或身份；这些由宿主提供。
- **尚无数据搬迁。** 把既有 store 或 provider 中的数据迁入 Mindcore 尚未实现，也没有升级工具：
  migration 早于已发布集合的数据库会被拒绝，而不是被转换。
- **每个数据库文件一个 store。** 每个打开的 `MindStore` 拥有一个连接池；同一文件上开两个 store
  不受支持。
- **migration 按 release 不可变。** 已发布的 migration 文件永不改变（lock 记录其 sha256）；新
  schema 以新增文件追加，在 release PR 中写入 lock，并在打 tag 时复核。已应用 migration 仍与内嵌
  集合一致的数据库可以正常重开；否则 `open` 会拒绝它，而不是猜测升级路径，见
  [存储兼容性](docs/storage-compatibility.md)。

仓库检查：CI 会运行格式化、clippy 与完整测试套件，并额外校验 `.sqlx` 离线缓存。完整本地命令块见
[docs/for-agents.md](docs/for-agents.md)，贡献约定见 [CONTRIBUTING.md](CONTRIBUTING.md)。
