# Gossip Glomers · Kotlin

使用 Kotlin/JVM 完成 [Gossip Glomers](https://fly.io/dist-sys/) 分布式系统实验。
已实现 Echo、Unique ID、Broadcast、Grow-Only Counter、多节点 Kafka（5b）和单节点事务（6a）；Kafka 5c 性能探索已阶段性收尾，事务多节点复制待实现。

## 使用

在 WSL 中运行，需要 JDK 25 和 Maelstrom；项目自带 Gradle Wrapper。

```sh
./lab test echo --log-stderr
./lab serve
```

实验代码位于 `src/main/kotlin/io/github/origincoding/gossipglomers/challenges/`。
命令说明见 [docs/commands.md](docs/commands.md)。

Kafka 可通过 `KAFKA_MODE=direct|hash|batch` 对比直接写入、Hash 路由和队列批处理，默认 `batch`。
本轮结论和验证限制见 [Kafka 性能探索记录](docs/kafka-performance.md)。

Kafka 低负载 benchmark 使用 `./scripts/kafka-bench run-all`，**最多执行一轮，禁止循环三轮或多终端并行**。
脚本自带 16 GiB Maelstrom 启动入口，不依赖 `/tmp`；重连 WSL 和完整命令见 [命令文档](docs/commands.md#kafka-benchmark最多执行一轮)。

基础设施及业务代码写法见 [docs/runtime.md](docs/runtime.md)。
