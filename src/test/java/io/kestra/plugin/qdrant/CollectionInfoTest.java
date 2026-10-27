package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.JacksonMapper;
import io.qdrant.client.grpc.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CollectionInfoTest extends QdrantTest {

    @BeforeEach
    void setUpCollection() throws Exception {
        if (!isQdrantAvailable()) return;
        var vectorParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();
        try (var client = qdrantClient()) {
            client.createCollectionAsync(collectionName, vectorParams).get();
        }
    }

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var task = CollectionInfo.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCollectionName(), is(collectionName));
        assertThat(output.getStatus(), notNullValue());
        assertThat(output.getVectorSize(), is((long) DIMENSION));
        assertThat(output.getPayloadSchema(), is(Map.of()));
    }

    @Test
    void shouldExposePayloadIndexes() throws Exception {
        var runContext = runContextFactory.of();
        Upsert.builder()
            .host(Property.ofValue(host)).port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(List.of(Map.of("id", 1, "vector", List.of(1f, 0f, 0f, 0f), "payload", Map.of("tenant_id", "team"))))
            .build().run(runContext);
        Collections.CollectionInfo expected;
        try (var client = qdrantClient()) {
            client.createPayloadIndexAsync(collectionName, "tenant_id", Collections.PayloadSchemaType.Keyword, null, true, null, Duration.ofSeconds(30)).get();
            expected = client.getCollectionInfoAsync(collectionName).get();
        }

        var output = CollectionInfo.builder()
            .host(Property.ofValue(host)).port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .build().run(runContext);
        var json = JacksonMapper.ofJson().valueToTree(output);

        assertThat(json.path("payloadSchema").path("tenant_id").path("dataType").asText("missing"), is("keyword"));
        assertThat(json.path("payloadSchema").path("tenant_id").path("points").asLong(-1), is(expected.getPayloadSchemaOrThrow("tenant_id").getPoints()));
    }

    @AfterEach
    void tearDown() {
        if (!isQdrantAvailable()) return;
        try (var client = qdrantClient()) {
            client.deleteCollectionAsync(collectionName).get();
        } catch (Exception ignored) {
        }
    }
}
