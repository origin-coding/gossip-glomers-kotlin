# Gossip Glomers · Kotlin

使用 Kotlin/JVM 完成 [Gossip Glomers](https://fly.io/dist-sys/) 分布式系统实验。
已实现 Echo、Unique ID、Broadcast、Grow-Only Counter、多节点 Kafka（5b）和读已提交事务（6c）；Kafka 5c 性能探索已阶段性收尾，事务采用异步批量复制。

这是一个学习项目，当前以整理实现、验证记录和学习笔记收尾，不再追加性能矩阵。

## 实验范围

| 实验                  | 当前实现                                             |
| --------------------- | ---------------------------------------------------- |
| 1 · Echo              | 请求处理与标准输入输出协议                           |
| 2 · Unique ID         | 使用 UUID v4 生成 ID                                 |
| 3 · Broadcast         | 可选拓扑、集合去重、批量传播和退避重试               |
| 4 · Grow-Only Counter | 基于 seq-kv 的读取与 CAS 更新                        |
| 5 · Kafka             | 多节点日志；保留 direct、hash、batch 三种写入方式    |
| 6 · Transactions      | 本地提交、版本化写集合、异步批量复制，目标为读已提交 |

实现范围不等于所有配置均已验收：Kafka 5c 没有完成全部性能矩阵；事务批量版最新的
两节点运行没有实际触发网络分区。具体证据和限制见下方文档。

## 使用

在 WSL 中运行，需要 JDK 25 和 Maelstrom；项目自带 Gradle Wrapper。

```sh
./lab test echo --log-stderr
./lab serve
```

单元测试不依赖 Maelstrom：

```sh
./gradlew test
```

实验代码位于 `src/main/kotlin/io/github/origincoding/gossipglomers/challenges/`。
命令说明见 [docs/commands.md](docs/commands.md)。

Kafka 可通过 `KAFKA_MODE=direct|hash|batch` 对比直接写入、Hash 路由和队列批处理，默认 `batch`。
本轮结论和验证限制见 [Kafka 性能探索记录](docs/kafka-performance.md)。

Kafka 低负载 benchmark 使用 `./scripts/kafka-bench run-all`，**最多执行一轮，禁止循环三轮或多终端并行**。
脚本自带 16 GiB Maelstrom 启动入口，不依赖 `/tmp`；重连 WSL 和完整命令见 [命令文档](docs/commands.md#kafka-benchmark最多执行一轮)。

基础设施及业务代码写法见 [docs/runtime.md](docs/runtime.md)。

## 项目边界

本项目围绕 Maelstrom 实验环境实现。事务状态和待复制队列保存在内存中，未实现重启恢复；
后台复制成功前，本地确认不代表其他节点已经可见，也不代表持久化完成。
当前实现不提供生产数据库所需的完整持久化、成员变更或资源治理能力。
原始测试结果、构建产物和本机 IDE 文件由 `.gitignore` 排除。
