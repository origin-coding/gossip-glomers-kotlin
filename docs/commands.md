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

## 查看结果

```sh
./lab serve
./lab serve --port 8081
```

默认访问 http://127.0.0.1:8080，按 Ctrl+C 停止。测试结果保存在 `store/`，不提交到 Git。

脚本优先使用 `MAELSTROM_BIN`，其次使用 PATH 中的 `maelstrom`，最后查找
`~/tools/maelstrom-0.2.4/maelstrom`。自定义安装位置可设置 `MAELSTROM_BIN` 为可执行文件的绝对路径。
