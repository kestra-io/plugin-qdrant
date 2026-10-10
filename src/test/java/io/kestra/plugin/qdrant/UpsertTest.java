package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.qdrant.client.grpc.Collections;
import io.kestra.core.serializers.FileSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class UpsertTest extends QdrantTest {

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
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        List<Map<String, Object>> points = List.of(
            Map.of(
                "id", 1,
                "vector", List.of(0.05f, 0.61f, 0.76f, 0.74f),
                "payload", Map.of("city", "Berlin", "country", "Germany")
            ),
            Map.of(
                "id", 2,
                "vector", List.of(0.19f, 0.81f, 0.75f, 0.11f),
                "payload", Map.of("city", "Paris", "country", "France")
            )
        );

        var task = Upsert.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(new Data(points))
            .batchSize(Property.ofValue(10))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getUpsertedCount(), is(2L));
    }

    @Test
    void runUpsertFromInternalStorage() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, Map.of(
                "id", 10,
                "vector", List.of(0.05f, 0.61f, 0.76f, 0.74f),
                "payload", Map.of("city", "Tokyo")
            ));
            FileSerde.write(output, Map.of(
                "id", 11,
                "vector", List.of(0.19f, 0.81f, 0.75f, 0.11f),
                "payload", Map.of("city", "Kyoto")
            ));
        }

        var storageUri = runContext.storage().putFile(tempFile);

        var task = Upsert.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(new Data(storageUri.toString()))
            .batchSize(Property.ofValue(1))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getUpsertedCount(), is(2L));
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
