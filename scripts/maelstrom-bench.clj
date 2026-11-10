(require '[maelstrom.core :as maelstrom]
         '[jepsen.tests.kafka :as kafka])

(binding [*out* *err*]
  (println "Benchmark runner: 16 GiB maximum heap; all correctness checks enabled; auxiliary Kafka order, unseen, and realtime-lag plots skipped."))

;; 只替换生成辅助图的函数，保留历史分析、错误判定和主要性能统计。
;; Maelstrom 0.2.4 的逐 key / 客户端绘图在高负载下会占用大量内存和磁盘。
(with-redefs [kafka/plot-unseen! (fn [& _] :skipped-auxiliary-plot)
              kafka/plot-realtime-lags! (fn [& _] :skipped-auxiliary-plot)
              kafka/render-order-viz! (fn [& _] :skipped-auxiliary-plot)]
  (apply maelstrom/-main *command-line-args*))

(shutdown-agents)
