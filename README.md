# Gossip Glomers · Kotlin

使用 Kotlin/JVM 完成 [Gossip Glomers](https://fly.io/dist-sys/) 分布式系统实验。
已实现 Echo、Unique ID、Broadcast、Grow-Only Counter 和多节点 Kafka（5b）；Kafka 5c 性能优化进行中，事务实验待实现。

## 使用

在 WSL 中运行，需要 JDK 25 和 Maelstrom；项目自带 Gradle Wrapper。

```sh
./lab test echo --log-stderr
./lab serve
```

实验代码位于 `src/main/kotlin/io/github/origincoding/gossipglomers/challenges/`。
命令说明见 [docs/commands.md](docs/commands.md)。

基础设施及业务代码写法见 [docs/runtime.md](docs/runtime.md)。
