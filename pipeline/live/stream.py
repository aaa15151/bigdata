"""Independent Spark 3.1+ microbatch job, submitted with the Kafka connector."""
import os
import re
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, types as T

run = os.environ['MOVIELENS_LIVE_RUN']
if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', run):
    raise ValueError('Use a safe unique run ID')
broker = os.environ['MOVIELENS_LIVE_BROKER']
base = os.environ.get('MOVIELENS_LIVE_ROOT', '/movie-live').rstrip('/') + '/' + run
spark = SparkSession.builder.appName('MovieLens-Live-Card-' + run).getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.sparkContext.setLogLevel('WARN')
schema = T.StructType([T.StructField('run_id', T.StringType())] + [
    T.StructField(name, T.LongType()) for name in ['event_seq','user_id','movie_id','rating','timestamp']])
events = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', broker)
    .option('subscribe', os.environ.get('MOVIELENS_LIVE_RATINGS', 'movielens-live-ratings'))
    .option('startingOffsets', 'earliest').option('maxOffsetsPerTrigger', 10000).load()
    .select(F.from_json(F.col('value').cast('string'), schema).alias('e')).select('e.*')
    .filter((F.col('run_id') == run) & F.col('rating').between(1, 5)
            & (F.col('user_id') > 0) & (F.col('movie_id') > 0) & (F.col('event_seq') >= 0)))

def commit(batch, batch_id):
    jvm = spark._jvm
    path = base + '/batches/batch-%020d' % batch_id
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(target):
        result = batch.agg(F.count('*').alias('rating_count'), F.avg('rating').alias('avg_rating'),
            F.countDistinct('user_id').alias('active_users'),
            F.avg(F.when(F.col('rating') <= 2, 1.0).otherwise(0.0)).alias('low_rating_ratio'))
        row = result.first()
        if not row['rating_count']:
            return
        result = spark.createDataFrame([row]).withColumn('run_id', F.lit(run)).withColumn('batch_id', F.lit(batch_id)).withColumn('committed_at', F.lit(datetime.now(timezone.utc).isoformat()))
        staging = path + '.staging'
        result.coalesce(1).write.mode('overwrite').json(staging)
        if not fs.rename(jvm.org.apache.hadoop.fs.Path(staging), target):
            raise RuntimeError('Cannot commit live batch')
    saved = spark.read.json(path)
    (saved.select(F.lit(run).alias('key'), F.to_json(F.struct(*[F.col(c) for c in saved.columns])).alias('value'))
        .write.format('kafka').option('kafka.bootstrap.servers', broker)
        .option('topic', os.environ.get('MOVIELENS_LIVE_UPDATES', 'movielens-live-updates')).save())
    print('LIVE_COMMITTED run=%s batch=%s' % (run, batch_id), flush=True)

query = (events.writeStream.option('checkpointLocation', base + '/checkpoint')
         .trigger(processingTime='5 seconds').foreachBatch(commit).start())
query.awaitTermination()
