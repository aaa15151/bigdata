package edu.movie;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class LiveApplicationTest {
    @Test void deduplicatesAndRetainsAllBatches() throws Exception {
        LiveApplication app = new LiveApplication();
        for (int i=0;i<35;i++) {
            var row=app.mapper.readTree("{\"run_id\":\"test\",\"batch_id\":"+i+",\"rating_count\":4,\"avg_rating\":3.5,\"active_users\":3,\"low_rating_ratio\":0.25,\"committed_at\":\"2026-11-24T00:00:00Z\"}");
            assertTrue(app.accept(row)); assertFalse(app.accept(row));
        }
        assertEquals(35, ((java.util.List<?>)app.snapshot().get("batches")).size());
        assertFalse(app.accept(app.mapper.readTree("{}")));
    }
}
