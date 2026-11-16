# 运行记录

这个目录存放博客引用的测试结果。每次引用都对应一个子目录，里面**只有两个文件**：

- `results.edn`：Maelstrom 的判定书，文章里的每个数字都来自它；
- `params.txt`：这次运行的命令行、git 提交、JDK 与 Maelstrom 版本。

目录名是 `<workload>/<开始时间>`，例如 `unique-ids/20261115T212202`。

## 为什么不直接提交 `store/`

`store/` 是 Maelstrom 的工作目录，一次运行动辄几 MB（`history.edn`、`jepsen.log`、图表和 HTML），而且只有本机才有意义，所以它在 `.gitignore` 里。

这里归档的是**结论**，不是过程。需要复现的读者拿着 `params.txt` 里的命令行重跑即可。

## 怎么产生

在仓库根目录：

```sh
./scripts/publish-run store/echo/20261114T154500.755+0800
```

第一个参数是 `store/` 下某次运行的目录，第二个参数可选，会给目录名加后缀（同一个实验跑了多种参数时用来区分）。

脚本会复制 `results.edn`、从 `jepsen.log` 里抓出原始命令行，并补上 git 提交、JDK 和 Maelstrom 版本，写进 `docs/runs/<workload>/<时间>/`。

## 约定

- 只放 `results.edn` 和 `params.txt`。如果某篇文章要讨论某次异常，才额外附上 `history.txt`。
- 文章里引用时用带 commit SHA 的永久链接，避免以后链接失效。
- 同一个实验的不同参数组合各占一个目录，靠 `params.txt` 区分。
