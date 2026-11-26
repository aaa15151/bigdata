import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class LiveRatingsProducer {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                "参数：BROKER FILE RUN_ID RATE LIMIT");
        }

        String broker = args[0];
        String file = args[1];
        String runId = args[2];
        int rate = Integer.parseInt(args[3]);
        long limit = Long.parseLong(args[4]);

        if (!runId.matches("[A-Za-z0-9_-]{1,80}")
                || rate <= 0 || limit <= 0) {
            throw new IllegalArgumentException("运行编号或发送参数无效");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("delivery.timeout.ms", "30000");
        props.put("max.block.ms", "15000");

        try (
            KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.US_ASCII))
        ) {
            long sent = 0;
            long started = System.nanoTime();
            String line;

            while (sent < limit && (line = reader.readLine()) != null) {
                String[] fields = line.split("::", -1);
                if (fields.length != 4) {
                    throw new IllegalArgumentException(
                        "评分格式错误，行号：" + (sent + 1));
                }

                int user = Integer.parseInt(fields[0]);
                int movie = Integer.parseInt(fields[1]);
                int rating = Integer.parseInt(fields[2]);
                long timestamp = Long.parseLong(fields[3]);

                if (user <= 0 || movie <= 0
                        || rating < 1 || rating > 5 || timestamp <= 0) {
                    throw new IllegalArgumentException(
                        "评分内容错误，行号：" + (sent + 1));
                }

                String json = String.format(
                    java.util.Locale.ROOT,
                    "{\"run_id\":\"%s\",\"event_seq\":%d,"
                    + "\"user_id\":%d,\"movie_id\":%d,\"rating\":%d,"
                    + "\"timestamp\":%d,\"sent_at_ms\":%d}",
                    runId, sent, user, movie, rating,
                    timestamp, System.currentTimeMillis());

                producer.send(new ProducerRecord<String, String>(
                    "movielens-live-ratings", runId, json)).get();

                sent++;
                long remaining = started
                    + (long) (sent * 1000000000.0 / rate)
                    - System.nanoTime();
                if (remaining > 0) {
                    java.util.concurrent.TimeUnit.NANOSECONDS.sleep(remaining);
                }

                if (sent % rate == 0) {
                    System.out.println(
                        "LIVE_SENT run=" + runId + " count=" + sent);
                }
            }

            producer.flush();
            System.out.println(
                "LIVE_FINISHED run=" + runId + " count=" + sent);
        }
    }
}
