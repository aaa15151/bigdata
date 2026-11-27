# 评分实时回放：已验证的 Java / Scala 路线

**当前网页暂停/继续功能请使用 [受控实时演示操作手册](../../docs/live-control-demo.md)**，启动 `ControlledRatingsProducer.java` 和 `control_server.py`。下文保留基础链路与旧自动发送器的操作，旧发送器不支持网页暂停，不要与受控发送器同时运行。

当前虚拟机 Python 为 3.5.2，因此以 `LiveRatingsProducer.java` 和 `stream_live.scala` 为主线。`replay.py` / `stream.py` 是早期备选，未在这次虚拟机验证，不要混用。

## 环境

- 既有 HDFS/YARN、Spark 3.1.2 / Scala 2.12、Kafka broker。
- 发送器复用原 `pipeline/streaming/lib` 中 Kafka 2.6 客户端及配套 Spark connector。缺少时参考 `scripts/fetch-streaming-deps.sh`。
- Windows 推送服务使用 JDK 17 和 Maven；不要把原集群的 Java 环境改成 JDK17。
- Kafka advertised.listeners 必须能从 Windows 和集群节点访问。

以下 Linux 命令在项目根目录执行。将 BROKER_IP 替换成实际 Kafka 地址。

## 1. 创建独立 Topic

```bash
export MOVIELENS_LIVE_BROKER='BROKER_IP:9092'
export KAFKA_HOME='/实际/Kafka安装目录'
for topic in movielens-live-ratings movielens-live-updates; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$MOVIELENS_LIVE_BROKER" --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done
```

不格式化 Kafka，不删除旧主题。已有集群不需要重新入湖或训练。

## 2. 准备数据与发送器

```bash
mkdir -p pipeline/live/data pipeline/live/classes
# 本地文件已存在时跳过 get
hdfs dfs -get /movie/raw/ratings.dat pipeline/live/data/ratings.dat
javac -encoding UTF-8 -cp 'pipeline/streaming/lib/*' -d pipeline/live/classes pipeline/live/LiveRatingsProducer.java
```

## 3. 启动 Spark（Linux 终端一，保持运行）

```bash
export MOVIELENS_LIVE_RUN="demo_$(date +%Y%m%d_%H%M%S)"
printf '%s\n' "$MOVIELENS_LIVE_RUN" > pipeline/live/current-run.txt
export HADOOP_CONF_DIR="$HOME/hadoop-3.2.0/etc/hadoop"
LIVE_JARS=$(printf '%s,' "$PWD"/pipeline/streaming/lib/*.jar)
LIVE_JARS=${LIVE_JARS%,}
"$HOME/spark-3.1.2-bin-hadoop3.2/bin/spark-shell" \
  --master yarn --deploy-mode client --name MovieLens-Live-Card \
  --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.sql.catalogImplementation=in-memory \
  --jars "$LIVE_JARS" -i pipeline/live/stream_live.scala
```

出现 `LIVE_STARTED` 后继续。记录运行编号。结果和 checkpoint 在 `/movie-live/<run_id>`；同一编号只运行一个 Spark 作业。

## 4. 启动推送服务（Windows PowerShell）

```powershell
# 将 JAVA_HOME 设置为本机 JDK17 安装目录
$env:JAVA_HOME='C:\实际路径\jdk-17'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
mvn -version
mvn -f realtime-service/pom.xml clean test package
$env:MOVIELENS_LIVE_BROKER='BROKER_IP:9092'
$env:MOVIELENS_LIVE_RUN='步骤3生成的编号'
java -jar realtime-service/target/movie-live-1.0.0.jar
```

默认监听 127.0.0.1:8088，允许看板来源 `http://127.0.0.1:5066` 和 `http://localhost:5066`。改端口时用 `MOVIELENS_LIVE_ORIGINS` 指定准确来源。环境变量需显式设置；`.env.example` 不会自动加载。

## 5. 启动页面

另开 Windows 终端，在项目根目录运行 `py -3.11 run.py`，访问 http://127.0.0.1:5066/#realtime 。不需要关闭上述服务。

## 6. 发送评分（Linux 终端二）

```bash
LIVE_RUN=$(cat pipeline/live/current-run.txt)
java -cp "pipeline/live/classes:pipeline/streaming/lib/*:$HOME/spark-3.1.2-bin-hadoop3.2/jars/*" \
  LiveRatingsProducer BROKER_IP:9092 pipeline/live/data/ratings.dat "$LIVE_RUN" 100 30000
```

只执行一次。重复对同一编号发送同一文件会重复统计；要重新从头演示，请使用新的编号并启动对应 Spark 和推送服务。页面“重新播放”按钮只重播已计算结果，不重复写数据。

## 7. 验证

发送器出现 LIVE_FINISHED count=30000；Spark 出现 LIVE_COMMITTED；页面显示推送已连接。

```powershell
$r=Invoke-RestMethod http://127.0.0.1:8088/api/live
$r.kafka_connected
$r.batches.Count
($r.batches | Measure-Object rating_count -Sum).Sum
```

累计评分应为 30000，批次数由运行速度决定，不一定等于本次记录的48。进入实时回放自动播放，支持暂停、重播、速度选择和图表滑块。播放的是实际结果，横轴是批次提交时间。

## 恢复与限制

- 服务重启从结果 Topic 的保留消息恢复，需选择同一 run_id；消息过期后不能保证恢复。
- HDFS 与 Kafka 不是跨系统事务；通知可能重复，服务按运行编号与批次编号去重。
- 当前服务把所选运行的全部批次保存在内存，适用于课程演示。长期运行需增加分页与持久化查询。
- WebSocket 在线不代表有新评分；最后批次时间用于判断数据新鲜度。
- 停止顺序：发送器 → 新 Spark 任务 → 推送服务。无需停止共用 Hadoop/Kafka。
