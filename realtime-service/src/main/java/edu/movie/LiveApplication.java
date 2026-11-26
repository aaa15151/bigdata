package edu.movie;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.socket.*;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

@SpringBootApplication
@RestController
public class LiveApplication extends TextWebSocketHandler {
    final ObjectMapper mapper = new ObjectMapper();
    final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    final LinkedHashMap<String, JsonNode> batches = new LinkedHashMap<>();
    volatile boolean running = true;
    volatile boolean kafkaConnected = false;
    volatile KafkaConsumer<String,String> consumer;
    Thread worker;
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    public static void main(String[] args) { SpringApplication.run(LiveApplication.class, args); }

    synchronized boolean accept(JsonNode node) {
        String selectedRun = env("MOVIELENS_LIVE_RUN", "");
        if (!selectedRun.isEmpty() &&
            !selectedRun.equals(node.path("run_id").asText())) return false;
        if (!node.path("run_id").isTextual() || !node.path("batch_id").isIntegralNumber()
            || !node.path("rating_count").isIntegralNumber() || node.path("rating_count").asLong() <= 0
            || !node.path("avg_rating").isNumber() || !node.path("active_users").isIntegralNumber()
            || !node.path("low_rating_ratio").isNumber() || !node.path("committed_at").isTextual()) return false;
        String key = node.get("run_id").asText() + ":" + node.get("batch_id").asLong();
        if (batches.containsKey(key)) return false;
        batches.put(key, node);
        // Keep all batches for the selected replay run.
        return true;
    }
    @GetMapping("/api/live")
    public synchronized Map<String,Object> snapshot() {
        return Map.of("type", "snapshot", "kafka_connected", kafkaConnected,
            "batches", new ArrayList<>(batches.values()), "source", "MovieLens 历史评分实时回放");
    }
    void send(WebSocketSession session, String value) {
        try { if (session.isOpen()) session.sendMessage(new TextMessage(value)); }
        catch (Exception error) { sessions.remove(session.getId()); try { session.close(); } catch (Exception ignored) {} }
    }
    void broadcast() throws Exception {
        String message = mapper.writeValueAsString(snapshot());
        for (WebSocketSession session : sessions.values()) send(session, message);
    }
    @Override public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        WebSocketSession safe = new ConcurrentWebSocketSessionDecorator(session, 2000, 65536);
        sessions.put(session.getId(), safe);
        send(safe, mapper.writeValueAsString(snapshot()));
    }
    @Override public void afterConnectionClosed(WebSocketSession session, CloseStatus status) { sessions.remove(session.getId()); }
    @PostConstruct public void start() {
        worker = new Thread(() -> {
            Properties p = new Properties();
            p.put("bootstrap.servers", env("MOVIELENS_LIVE_BROKER", "namenode:9092"));
            p.put("group.id", "movie-live-" + UUID.randomUUID());
            p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            p.put("auto.offset.reset", "earliest");
            p.put("enable.auto.commit", "false");
            p.put("default.api.timeout.ms", "5000");
            while (running) {
                try (KafkaConsumer<String,String> client = new KafkaConsumer<>(p)) {
                    consumer = client;
                    client.subscribe(List.of(env("MOVIELENS_LIVE_UPDATES", "movielens-live-updates")));
                    long heartbeat = 0;
                    while (running) {
                        boolean changed = false;
                        for (ConsumerRecord<String,String> record : client.poll(Duration.ofSeconds(1))) {
                            try { changed |= accept(mapper.readTree(record.value())); }
                            catch (Exception invalid) { System.err.println("Invalid live message at offset " + record.offset()); }
                        }
                        long now = System.currentTimeMillis();
                        if (changed || now - heartbeat > 5000) {
                            try { client.listTopics(Duration.ofSeconds(3)); kafkaConnected = true; }
                            catch (Exception unavailable) { kafkaConnected = false; }
                            broadcast(); heartbeat = now;
                        }
                    }
                } catch (WakeupException stop) { if (!running) break; }
                  catch (Exception error) { kafkaConnected = false; try { broadcast(); Thread.sleep(3000); } catch (Exception ignored) {} }
                finally { consumer = null; }
            }
        }, "live-kafka-consumer");
        worker.setDaemon(true); worker.start();
    }
    @PreDestroy public void stop() throws InterruptedException {
        running = false;
        KafkaConsumer<String,String> active = consumer;
        if (active != null) active.wakeup();
        if (worker != null) worker.join(5000);
    }
    @Configuration
    @EnableWebSocket
    static class SocketConfig implements WebSocketConfigurer {
        private final LiveApplication app;
        SocketConfig(LiveApplication app) { this.app = app; }
        @Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
            registry.addHandler(app, "/ws/live").setAllowedOrigins(env("MOVIELENS_LIVE_ORIGINS",
                "http://127.0.0.1:5066,http://localhost:5066").split(","));
        }
    }
}
