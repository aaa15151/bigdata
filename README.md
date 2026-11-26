# MovieLens 大数据分析与评分实时回放

基于 MovieLens 1M 的课程项目，使用 Hadoop / Hive / Spark / Kafka 完成数据分析，通过 Flask、Spring Boot WebSocket 和 ECharts 展示结果。

## 快速开始：查看已完成的分析结果

需要 Git、Python 3.11 或以上。

```powershell
git clone https://github.com/aaa15151/bigdata.git
cd bigdata
py -3.11 run.py
```

Linux/macOS 使用 `python3 run.py`。打开 http://127.0.0.1:5066/ 。首次启动会建立虚拟环境并安装 Flask。

仓库自带聚合结果与推荐快照，可查看离线分析、ALS 推荐、相似电影、用户画像及电影关系网，无需重新训练。实时回放页需要另外启动推送服务并接入 Kafka；只运行 `run.py` 不会生成实时评分。

## 功能

- 离线分析：评分趋势、评分分布、活跃用户、日历与影片热度。
- ALS 推荐：6,040 位用户、60,400 条已生成推荐。
- 评分实时回放：独立 Kafka Topic 接收限速评分，Spark Scala 微批统计，HDFS 保存结果，Spring Boot 推送至统一回放面板。
- 演示播放：进入回放页后按批次展开，支持暂停、继续、重新播放、速度选择、时间范围缩放。它播放真实链路产出的聚合结果，不重新向 Kafka 写入评分。
- 用户画像：年龄切换的中文横向类型排名、性别类型偏好、类型 Top10、每日评分量与移动平均。
- 扩展分析：RDD 相似电影、GraphX 电影关系网。

## 两种使用方式

| 方式 | 依赖 | 能做什么 |
| --- | --- | --- |
| 结果浏览 | Python + Flask | 查看随仓库提供的分析和推荐结果 |
| 实时链路及演示 | 结果浏览 + Kafka + Spark/HDFS + JDK17/Maven | 接收评分批次并在回放页动态展示 |

实时推送服务只提供 Kafka 保留期间可恢复的数据；不是永久数据库。每次独立演示使用新的 `run_id`，Spark 和推送服务必须选择同一个编号。

## 实时链路

```text
ratings.dat → Java 限速发送 → Kafka ratings
→ Spark Scala foreachBatch → HDFS 批次结果
→ Kafka updates → Spring Boot WebSocket
→ ECharts 评分量 / 均分 / 低分告警 / 活跃用户
```

[实时链路启动手册](pipeline/live/README.md) · [部署与复现](docs/deployment.md) · [三节点虚拟机](docs/vmware.md) · [本次交付记录](docs/release-2026-11-26.md)

## 目录

| 目录 | 内容 |
| --- | --- |
| dashboard | Flask、ECharts、聚合结果快照 |
| realtime-service | JDK17 Spring Boot 推送服务及单元测试 |
| pipeline/live | 本次验证过的 Java 发送器、Scala 流任务 |
| pipeline/streaming | 原版事件时间五分钟窗口流水线 |
| pipeline/ingest、offline、als、sql | 入湖、离线统计、推荐与 Hive |
| infra | 集群配置参考；不包含虚拟磁盘 |
| verification | 数据与源码验证 |
| docs | 部署、结果口径、交付记录 |

## 检查

```bash
python -m pip install -r dashboard/requirements.txt
python verification/verify_snapshot.py
python verification/verify_extensions.py
mvn -f realtime-service/pom.xml test package
```

CI 检查 Python、前端 JavaScript、Shell 和 Java 服务。CI 不启动完整 Hadoop/Kafka 集群。

## 数据与使用范围

数据来源：[GroupLens MovieLens 1M](https://grouplens.org/datasets/movielens/1m/)，请遵守其数据使用条款。电影可属于多个类型；类型占比按评分归属次数计算，不是用户占比。评分回放使用既有数据，处理时间和原始评分时间不同。

源码沿用原项目及课程实现，保留原来源说明；本次整理不擅自为第三方代码追加开源许可。ECharts 许可保留在 dashboard/static 中。技术路线参考 [Distributed Spark Movie Recommender](https://github.com/zubiiabbasi/distributed-spark-movielens-recommender)。

不会提交 Python 运行时、虚拟机磁盘、原始数据、Maven 缓存、JAR 构建产物、日志或本地备份。