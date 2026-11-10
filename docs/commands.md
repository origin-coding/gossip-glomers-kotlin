# 命令

## 测试实验

```sh
./lab test echo --log-stderr
./lab test broadcast --node-count 5 --time-limit 20
```

每次测试先增量构建，再运行 Maelstrom。省略实验名时默认使用 `echo`。
可选实验：`echo`、`unique-id`、`broadcast`、`g-counter`、`kafka`、`txn`。
默认测试 10 秒，Echo 使用 1 个节点，其余使用 3 个节点；追加参数直接传给 Maelstrom。
这些默认值用于快速验证，完整验收参数按各实验题目设置。

Kafka 已完成到 5b，使用 `lin-kv` 共享日志和消费进度；按 5b 参数验收：

```sh
./lab test kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

## Kafka 写入模式

`KAFKA_MODE` 选择 Kafka 的写入策略，未设置时默认 `batch`，沿用已有的队列批处理。

| 模式 | 写入策略 |
| --- | --- |
| `direct` | 接收请求的节点直接读取 lin-kv 并 CAS 追加单条消息，冲突时重试 |
| `hash` | 按 key hash 转发给 owner，owner 按 key 加锁并追加单条消息 |
| `batch` | 按 key hash 转发给 owner，每个 key 的队列和消费协程合并已有消息后追加 |

三种模式共用日志格式和 CAS 重试逻辑；poll、消费进度提交和查询使用相同实现。
`batch` 每批最多 64 条，不增加主动等待窗口。所有节点必须使用相同的 `KAFKA_MODE`；
以下命令的环境变量会传递给 Maelstrom 启动的所有节点。

```sh
KAFKA_MODE=direct ./lab test kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
KAFKA_MODE=hash ./lab test kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
KAFKA_MODE=batch ./lab test kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

对比时为三个模式使用相同的节点数、并发、rate 和时长，并记录模式及对应的结果目录。
增加并发时也要检查实际吞吐是否受到 rate 限制。当前 benchmark 最多执行一轮，不做重复轮次。

## Kafka benchmark：最多执行一轮

本轮探索已经收尾；结果、检查器异常及验证范围见 [性能探索记录](kafka-performance.md)。
以下命令作为使用记录保留，本次不再补跑。

> **警告：最多执行一轮。** 一轮是下面 10 组负载各执行 `direct`、`hash`、`batch` 一次，共 30 次测试。
> 不要使用三轮循环、`--test-count 3`，也不要在多个终端并行启动。
> 此前连续三轮期间发生了 WSL 断开；16 GiB 堆上限不代表可以连续扩大负载和重复运行。
> 即使中途失败或重连 WSL，也不要自动从头再跑一轮。

旧的原始 D 负载为 2 节点、64 并发、rate 8000、30 秒；该高压矩阵不再作为当前运行入口。
下面统一使用 5 秒低负载矩阵。5 秒是主要负载时长，构建、收尾和分析时间另算。

### 持久化的 16 GiB Maelstrom 启动入口

`scripts/maelstrom-bench` 是项目内的可执行脚本，替代之前放在 `/tmp` 的临时入口；
WSL 重连或重启后仍然可用。它使用 `java -Xmx16g`，只给 Maelstrom 主进程设置堆上限。
业务节点使用自身的 JVM 配置。16 GiB 是堆上限，不是立即预分配，也不是整个测试的总内存上限。

默认 jar 路径为 `~/tools/maelstrom-0.2.4/lib/maelstrom.jar`，自定义位置可设置：

```sh
export MAELSTROM_JAR=/absolute/path/to/maelstrom.jar
```

入口搭配 `scripts/maelstrom-bench.clj`，仅跳过 Kafka 的 order、unseen 和 realtime-lag 辅助绘图。
历史正确性分析和错误判定全部保留；`results.edn`、吞吐/延迟图和网络统计正常生成。
Maelstrom 0.2.4 的辅助绘图曾触发 `Java heap space`，并按客户端与 key 生成数万张图片；
这些辅助图在本 benchmark 中不作为交付结果。

### 一轮完整矩阵

在 WSL 的项目根目录执行：

```sh
./scripts/kafka-bench --help
./scripts/kafka-bench run-all
```

不再需要粘贴或 `source` 聊天中的 `run_case` / `run_all` 函数，也不需要导出临时的
`MAELSTROM_BIN=/tmp/gossip-maelstrom-bench`。矩阵脚本会为每次测试设置正确的入口和模式。

| 编号 | 节点 | 总并发客户端 | 目标 rate（操作/秒） | 网络延迟（ms） | 每 key 写入上限 | 主要负载时长 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| S-A | 2 | 4 | 1000 | 0 | 16 | 5 秒 |
| S-B | 2 | 16 | 1000 | 0 | 16 | 5 秒 |
| S-C | 2 | 32 | 2000 | 0 | 16 | 5 秒 |
| S-D | 2 | 16 | 1000 | 5 | 16 | 5 秒 |
| S-E | 2 | 16 | 1000 | 0 | 256 | 5 秒 |
| S-F | 5 | 16 | 1000 | 0 | 16 | 5 秒 |
| S-G | 5 | 32 | 2000 | 0 | 16 | 5 秒 |
| S-H | 5 | 16 | 1000 | 5 | 256 | 5 秒 |
| S-I | 2 | 32 | 4000 | 0 | 16 | 5 秒 |
| S-J | 5 | 32 | 4000 | 0 | 16 | 5 秒 |

节点数不包含 lin-kv 服务。并发是总客户端数量，不是每节点数量；rate 也是全局目标，实际吞吐可能更低。
网络延迟默认使用固定分布。每 key 写入上限是生成器参数，不是批次或分块大小；代码中的批次上限仍是 64。

日志和带表头的参数/结果索引保存在 `store/kafka-bench-low/`；每组依次运行三个模式，失败即停止并记录退出码。
`store/.kafka-bench.lock` 防止同时运行，`store/.kafka-bench-started` 防止启动第二轮。
启动标记在成功、失败、中断、WSL 重连后都会保留；脚本不提供重复轮次、自动重试或强制重跑选项。
标记存在时先检查已有日志、结果和资源，不要为了重跑直接删除标记。

### 单次命令的完整写法

需要单独定位某个模式时，等价的 S-A 单次命令如下；这不是额外三轮测试，
不要在完整矩阵完成后继续追加执行。修改负载时须保持三个模式使用相同参数。

```sh
MAELSTROM_BIN="$PWD/scripts/maelstrom-bench" KAFKA_MODE=direct ./lab test kafka --node-count 2 --concurrency 4 --rate 1000 --latency 0 --max-writes-per-key 16 --time-limit 5
MAELSTROM_BIN="$PWD/scripts/maelstrom-bench" KAFKA_MODE=hash ./lab test kafka --node-count 2 --concurrency 4 --rate 1000 --latency 0 --max-writes-per-key 16 --time-limit 5
MAELSTROM_BIN="$PWD/scripts/maelstrom-bench" KAFKA_MODE=batch ./lab test kafka --node-count 2 --concurrency 4 --rate 1000 --latency 0 --max-writes-per-key 16 --time-limit 5
```

### 重连 WSL 后怎么启动

先在 Windows PowerShell 进入 Ubuntu：

```powershell
wsl.exe -d Ubuntu-24.04
```

再在 WSL 中进入项目，确认 Java 和脚本入口：

```sh
cd /home/origin-coding/gossip-glomers-kotlin
java -version
./scripts/kafka-bench --help
```

准备开始这一轮时才运行：

```sh
./scripts/kafka-bench run-all
```

如果此前已经启动过一轮，脚本会拒绝再次启动；重连不会清除启动标记。
查看结果可在另一个 WSL 终端进入项目后执行：

```sh
cd /home/origin-coding/gossip-glomers-kotlin
unset MAELSTROM_BIN
./lab serve
```

浏览器访问 http://localhost:8080。`serve` 只查看结果，不启动 benchmark。

## 广播拓扑

Topology 只属于 Broadcast。未设置时使用星形拓扑，沿用之前的广播策略。
`default` 表示采用 Maelstrom 在 `topology` 消息中提供的邻接表；`fanout` 表示按
`node_ids` 顺序生成双向树，`BROADCAST_FANOUT` 指定每个节点最多有几个子节点，默认 3。

```sh
BROADCAST_TOPOLOGY=default ./lab test broadcast --node-count 5
BROADCAST_TOPOLOGY=star ./lab test broadcast --node-count 25
BROADCAST_TOPOLOGY=fanout BROADCAST_FANOUT=3 ./lab test broadcast --node-count 25
```

带分区的广播验证：

```sh
./lab test broadcast --node-count 5 --rate 100 --time-limit 20 --nemesis partition
```

## 基础设施测试

```sh
./gradlew test
```

## 查看结果

```sh
./lab serve
./lab serve --port 8081
```

默认访问 http://127.0.0.1:8080，按 Ctrl+C 停止。测试结果保存在 `store/`，不提交到 Git。

脚本优先使用 `MAELSTROM_BIN`，其次使用 PATH 中的 `maelstrom`，最后查找
`~/tools/maelstrom-0.2.4/maelstrom`。自定义安装位置可设置 `MAELSTROM_BIN` 为可执行文件的绝对路径。
