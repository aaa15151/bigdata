# 扩展模块运行与验收

## 本地演示

Python 3.11 或更新版本，运行 `python run.py`。默认地址 http://127.0.0.1:5066/ 。完整快照包含原有统计、ALS、回放以及 M1/M2/M4，不需要 Hadoop。首次启动需要联网安装 Flask。

## 集群计算顺序

先按 deployment.md 准备原有清洗结果。NameNode 项目根目录依次运行：

```bash
bash pipeline/rdd/run-m1.sh
bash pipeline/sql/run-m2.sh
bash pipeline/graph/run-m4.sh
```

M2 需要已启动的 Hive Metastore（thrift://namenode:9083）。使用 Derby 时不要同时用原有嵌入式 Hive CLI 打开数据库；CLI 指定 `--hiveconf hive.metastore.uris=thrift://namenode:9083`。

默认 M4 输出 graph_v3，前端也读取这一版。各作业拒绝覆盖输出；不要为了重跑删除现有目录，应先检查成功标记和日志，或使用独立实验目录。

## 已完成的集群验收

| 模块 | 本次结果 |
| --- | --- |
| M1 | 有效评分 1,000,209，脏数据 0，相似推荐 23,900 |
| M2 | 总计 1,000,209，类型榜单 180 条，年月分区裁剪已验证 |
| M4 | 1,400 节点、9,632 对关系、31 社区、1 连通分量、展示 100 节点 |

GraphX PageRank 使用无权拓扑，边的共同喜欢人数用于筛选和展示；社区由标签传播产生，不由电影类型预先指定。标签传播存在迭代和分布式运行差异，其他运行结果不保证完全一致。

页面日期为 2024 年 1 月至 2026 年 8 月的压缩演示映射，原始统计仍基于 MovieLens 历史事件。不要以展示坐标重新解释 7 日窗口或真实采集日期。

## 验证

```bash
python verification/verify_snapshot.py
python verification/verify_extensions.py
```

需要集群重新生成快照时，先设置部署文档中的 MOVIELENS_WEBHDFS、MOVIELENS_DATANODES、MOVIELENS_KAFKA_BOOTSTRAP，再运行 `python scripts/export-snapshot.py`。不要提交本机 Python 环境、原始数据、日志或虚拟机磁盘。
