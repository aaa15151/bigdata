# 受控评分实时演示

本版本使用真实 Kafka → Spark → HDFS/Kafka → Spring Boot WebSocket 链路，替代前端定时动画。评分来源是 MovieLens 既有数据，不是线上新评分。暂停停止新增输入，已经发送的数据仍会继续处理。继续仅支持同一发送器进程，停止或完成后须准备新运行；不支持进程崩溃后断点续发。

## 前提与终端

先按 [基础启动手册](../pipeline/live/README.md) 配置 Hadoop/YARN、Kafka 两个主题、Spark connector、ratings.dat 和 Spring Boot 构建。集群使用 Java 8，Windows Spring Boot 使用 Java 17。下列路径和 IP 是本次验证环境，其他电脑需替换。

保留独立终端：虚拟机发送器、虚拟机控制服务、虚拟机 Spark；Windows SSH 隧道、Spring Boot、Flask；另留空闲终端执行检查。

已有演示先关闭网页，停止旧发送器、控制服务、Spark 和 Spring Boot，保留 Hadoop、Kafka、SSH 隧道及 Flask。可向旧控制接口 POST /stop（带 X-Replay-Control: 1），确认 STOPPED；控制服务 Ctrl+C，Spark 退出后确认旧 YARN 应用结束。不要删除旧数据或复用旧控制目录。

## 1. 虚拟机：新编号与发送器

在项目根目录执行：

```bash
cd /home/hadoop/movielens-bigdata-dashboard-1-main
mkdir -p pipeline/live/classes
javac -encoding UTF-8 -cp 'pipeline/streaming/lib/*' -d pipeline/live/classes pipeline/live/ControlledRatingsProducer.java
NEW_RUN="live_demo_$(date +%Y%m%d_%H%M%S)"
printf '%s\n' "$NEW_RUN" > pipeline/live/active-control-run.txt
java -cp "pipeline/live/classes:pipeline/streaming/lib/*:$HOME/spark-3.1.2-bin-hadoop3.2/jars/*" ControlledRatingsProducer 192.168.17.140:9092 pipeline/live/data/ratings.dat "$NEW_RUN" 50 30000 "$PWD/pipeline/live/control/$NEW_RUN"
```

出现 CONTROL_READY，初始 PAUSED、sent=0。50 是目标每秒发送量，30000 是上限；实际速度受确认延迟影响。

## 2. 虚拟机：控制服务（另一终端）

```bash
cd /home/hadoop/movielens-bigdata-dashboard-1-main
ACTIVE_RUN=$(cat pipeline/live/active-control-run.txt)
python3 pipeline/live/control_server.py --control-dir "$PWD/pipeline/live/control/$ACTIVE_RUN" --port 8090
```

仅监听 127.0.0.1，使用 SSH 隧道。GET /status；POST /resume、/pause、/stop 要求 X-Replay-Control: 1。202 只表示命令已写入，需查询状态确认；终止状态或过期状态返回 409。

## 3. 虚拟机：Spark（另一终端）

```bash
cd /home/hadoop/movielens-bigdata-dashboard-1-main
export MOVIELENS_LIVE_RUN=$(cat pipeline/live/active-control-run.txt)
export MOVIELENS_LIVE_BROKER=192.168.17.140:9092
export HADOOP_CONF_DIR="$HOME/hadoop-3.2.0/etc/hadoop"
LIVE_JARS=$(printf '%s,' "$PWD"/pipeline/streaming/lib/*.jar)
LIVE_JARS=${LIVE_JARS%,}
~/spark-3.1.2-bin-hadoop3.2/bin/spark-shell --master yarn --deploy-mode client --name MovieLens-Live-Control --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g --conf spark.sql.shuffle.partitions=2 --conf spark.sql.catalogImplementation=in-memory --jars "$LIVE_JARS" -i pipeline/live/stream_live.scala
```

等待 LIVE_STARTED，确认 YARN 应用 RUNNING。新任务从 earliest 读取并过滤旧运行事件，首次准备可能较慢。触发间隔 5 秒不代表严格每 5 秒完成。

## 4. Windows：隧道、网页、推送服务

仅在隧道未运行时，新 PowerShell 执行并保持打开：

```powershell
ssh -N -L 127.0.0.1:8090:127.0.0.1:8090 -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 hadoop@192.168.17.140
```

Flask 在项目根目录用已安装依赖的 Python 启动：

```powershell
python dashboard/app.py --mode snapshot --port 5066
```

已有 Flask 保持运行即可。本机打包环境可用 `.\runtime\python.exe` 替代 python，runtime 不随仓库发布。

在另一个项目根目录 PowerShell 中，确认 `java -version` 为 17，然后启动：

```powershell
$control = Invoke-RestMethod 'http://127.0.0.1:5066/api/live-control/status'
$control | Format-List run_id,state,sent,limit
$env:MOVIELENS_LIVE_RUN = $control.run_id
$env:MOVIELENS_LIVE_BROKER = '192.168.17.140:9092'
java -jar realtime-service/target/movie-live-1.0.0.jar
```

## 5. 验收与演示

空闲 PowerShell 检查：

```powershell
Invoke-RestMethod 'http://127.0.0.1:5066/api/live-control/status' | Format-List run_id,state,sent,limit
$live = Invoke-RestMethod 'http://127.0.0.1:8088/api/live'
$live.kafka_connected
@($live.batches).Count
```

初始应 PAUSED、sent=0、Kafka=True、结果批次=0。打开 http://127.0.0.1:5066/#offline，强制刷新并等待状态加载，再进入实时回放。首次进入触发发送；若仍暂停，点击继续发送。观察 30～60 秒，再暂停、等待结果稳定、继续。刷新网页不会清零；同一页面切换回来也不是新运行。

发送量来自控制接口轮询，分析结果来自 WebSocket 推送。每批评分数不是累计发送量。有效结果批次数可能小于 Spark 最后批次编号。暂停后待处理追上，可用下面命令核对发送量与累计处理量：

```powershell
$control = Invoke-RestMethod 'http://127.0.0.1:5066/api/live-control/status'
$live = Invoke-RestMethod 'http://127.0.0.1:8088/api/live'
$control.sent
($live.batches | Measure-Object -Property rating_count -Sum).Sum
```

当前实时链路只更新评分分析区域，不会实时重新训练 ALS 或更新全部离线模块。控制接口用于本地可信演示，不要直接暴露到公网。
