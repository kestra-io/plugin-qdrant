package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.grpc.Collections;
import io.kestra.core.serializers.FileSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.qdrant.client.PointIdFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            .points(points)
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
            .points(storageUri.toString())
            .batchSize(Property.ofValue(1))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getUpsertedCount(), is(2L));
    }

    @Test
    void upsertsNamedDenseAndSparseVectors() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();
        var hybrid = collectionName + "_hybrid";

        try (var client = qdrantClient()) {
            client.createCollectionAsync(
                Collections.CreateCollection.newBuilder()
                    .setCollectionName(hybrid)
                    .setVectorsConfig(Collections.VectorsConfig.newBuilder()
                        .setParamsMap(Collections.VectorParamsMap.newBuilder().putMap(
                            "text_dense",
                            Collections.VectorParams.newBuilder().setSize(DIMENSION).setDistance(Collections.Distance.Cosine).build()
                        ).build()))
                    .setSparseVectorsConfig(Collections.SparseVectorConfig.newBuilder()
                        .putMap("text_sparse", Collections.SparseVectorParams.newBuilder().build()))
                    .build()
            ).get();

            var output = Upsert.builder()
                .host(Property.ofValue(host))
                .port(Property.ofValue(port))
                .collectionName(Property.ofValue(hybrid))
                .points(List.of(Map.of(
                    "id", 7,
                    "vectors", Map.of(
                        "text_dense", List.of(1.0f, 0.0f, 0.0f, 0.0f),
                        "text_sparse", Map.of("indices", List.of(12, 104, 982), "values", List.of(0.84f, 1.23f, 0.41f))
                    )
                )))
                .build()
                .run(runContext);

            assertThat(output.getUpsertedCount(), is(1L));

            var stored = client.retrieveAsync(
                hybrid, List.of(PointIdFactory.id(7L)), false, true, null
            ).get().getFirst().getVectors().getVectors().getVectorsMap();

            var sparse = stored.get("text_sparse").getSparse();
            assertThat(sparse.getIndicesList(), is(List.of(12, 104, 982)));
            assertThat(sparse.getValuesList(), is(List.of(0.84f, 1.23f, 0.41f)));
            // the vector is unit length already: Qdrant normalizes vectors of a Cosine collection
            assertThat(stored.get("text_dense").getDense().getDataList(), is(List.of(1.0f, 0.0f, 0.0f, 0.0f)));

            client.deleteCollectionAsync(hybrid).get();
        }
    }

    @Test
    void invalidSparseVectorsAreRejectedWithThePointAndVectorName() {
        var runContext = runContextFactory.of();

        assertRejected(runContext, Map.of("indices", List.of(1, 2), "values", List.of(0.5f)), "must have the same number of elements");
        assertRejected(runContext, Map.of("indices", List.of(1, 1), "values", List.of(0.5f, 0.5f)), "must not contain duplicate indices");
        assertRejected(runContext, Map.of("indices", List.of(-1), "values", List.of(0.5f)), "must contain non-negative indices");
        assertRejected(runContext, Map.of("indices", List.of(1.5), "values", List.of(0.5f)), "must contain whole numbers");
        assertRejected(runContext, Map.of("indices", List.of(1), "values", List.of(Float.NaN)), "must contain finite numbers only");
        assertRejected(runContext, Map.of("indices", List.of(), "values", List.of()), "at least one entry");
        assertRejected(runContext, Map.of("indices", List.of(1)), "needs both an 'indices' list and a 'values' list");
        assertRejected(runContext, Map.of("indices", List.of(1), "values", List.of(0.5f), "extra", 1), "unknown field 'extra'");
    }

    private void assertRejected(RunContext runContext, Map<String, Object> sparse, String expected) {
        var task = Upsert.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(List.of(Map.of("id", 42, "vectors", Map.of("text_sparse", sparse))))
            .build();

        var failure = assertThrows(Exception.class, () -> task.run(runContext));
        var message = rootMessage(failure);
        assertThat(message, containsString(expected));
        assertThat(message, containsString("Point '42': sparse vector 'text_sparse'"));
    }

    private static String rootMessage(Throwable throwable) {
        var current = throwable;
        var message = String.valueOf(current.getMessage());
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
            message = message + " | " + current.getMessage();
        }
        return message;
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
