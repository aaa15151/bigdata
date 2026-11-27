import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.*;

public class ControlledRatingsProducer {
    private static void writeAtomic(Path path, String text) throws IOException {
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        Files.write(temporary, text.getBytes(StandardCharsets.UTF_8));
        Files.move(temporary, path,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
    }

    private static void status(Path directory, String run,
                               String state, long sent, long limit)
            throws IOException {
        String json = String.format(Locale.ROOT,
            "{\"run_id\":\"%s\",\"state\":\"%s\","
            + "\"sent\":%d,\"limit\":%d,\"updated_at_ms\":%d}",
            run, state, sent, limit, System.currentTimeMillis());
        writeAtomic(directory.resolve("status.json"), json);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                "Usage: BROKER FILE RUN_ID RATE LIMIT CONTROL_DIR");
        }

        String broker = args[0];
        String source = args[1];
        String run = args[2];
        int rate = Integer.parseInt(args[3]);
        long limit = Long.parseLong(args[4]);
        Path directory = Paths.get(args[5]).toAbsolutePath();

        if (!run.matches("[A-Za-z0-9_-]{1,80}")
                || rate < 1 || rate > 10000 || limit < 1) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        // One unique control directory per run. Never overwrite an old run.
        Files.createDirectories(directory.getParent());
        Files.createDirectory(directory);

        Path command = directory.resolve("command.txt");
        writeAtomic(command, "PAUSE");
        long sent = 0;
        status(directory, run, "PAUSED", sent, limit);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", broker);
        properties.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");
        properties.put("delivery.timeout.ms", "30000");
        properties.put("max.block.ms", "15000");

        System.out.println("CONTROL_READY run=" + run
            + " directory=" + directory);

        try (
            KafkaProducer<String,String> producer =
                new KafkaProducer<>(properties);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(source), StandardCharsets.US_ASCII))
        ) {
            while (sent < limit) {
                String action = new String(
                    Files.readAllBytes(command),
                    StandardCharsets.UTF_8).trim();

                if ("STOP".equals(action)) {
                    status(directory, run, "STOPPED", sent, limit);
                    System.out.println("CONTROL_STOPPED sent=" + sent);
                    return;
                }

                // Missing/invalid commands never start sending.
                if (!"RUN".equals(action)) {
                    status(directory, run, "PAUSED", sent, limit);
                    Thread.sleep(250);
                    continue;
                }

                long tick = System.nanoTime();
                String line = reader.readLine();
                if (line == null) {
                    status(directory, run, "SOURCE_EXHAUSTED", sent, limit);
                    System.out.println("SOURCE_EXHAUSTED sent=" + sent);
                    return;
                }

                String[] fields = line.split("::", -1);
                if (fields.length != 4) {
                    throw new IllegalArgumentException("Malformed rating");
                }

                int user = Integer.parseInt(fields[0]);
                int movie = Integer.parseInt(fields[1]);
                int rating = Integer.parseInt(fields[2]);
                long timestamp = Long.parseLong(fields[3]);

                if (user <= 0 || movie <= 0 || rating < 1
                        || rating > 5 || timestamp <= 0) {
                    throw new IllegalArgumentException("Invalid rating");
                }

                String json = String.format(Locale.ROOT,
                    "{\"run_id\":\"%s\",\"event_seq\":%d,"
                    + "\"user_id\":%d,\"movie_id\":%d,\"rating\":%d,"
                    + "\"timestamp\":%d,\"sent_at_ms\":%d}",
                    run, sent, user, movie, rating,
                    timestamp, System.currentTimeMillis());

                producer.send(new ProducerRecord<String,String>(
                    "movielens-live-ratings", run, json)).get();

                sent++;
                if (sent == 1 || sent % rate == 0) {
                    status(directory, run, "RUNNING", sent, limit);
                    System.out.println("CONTROL_SENT run=" + run
                        + " sent=" + sent);
                }

                long remaining = 1000000000L / rate
                    - (System.nanoTime() - tick);
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
            }

            producer.flush();
            status(directory, run, "COMPLETED", sent, limit);
            System.out.println("CONTROL_COMPLETED sent=" + sent);
        } catch (Exception error) {
            status(directory, run, "ERROR", sent, limit);
            throw error;
        }
    }
}
