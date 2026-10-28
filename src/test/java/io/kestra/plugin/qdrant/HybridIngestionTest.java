package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Builds a hybrid collection, loads it and queries it with plugin tasks only, no direct SDK calls to prepare data.
public class HybridIngestionTest extends QdrantTest {

    @Test
    void createUpsertAndHybridQueryWithPluginTasksOnly() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        CreateCollection.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vectorName(Property.ofValue("text_dense"))
            .vectorSize(Property.ofValue(DIMENSION))
            .distance(Property.ofValue(Distance.COSINE))
            .sparseVectors(List.of(
                CreateCollection.SparseVectorSpace.builder().name(Property.ofValue("text_sparse")).build()
            ))
            .build()
            .run(runContext);

        // same fixture as HybridQueryTest: dense-only ranks 1, 2, 4, 3 and sparse-only ranks 3, 4, 1
        var upserted = Upsert.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(List.of(
                point(1, List.of(0.95f, 0.05f, 0.0f, 0.0f), List.of(5), List.of(0.1f)),
                point(2, List.of(0.6f, 0.4f, 0.0f, 0.0f), null, null),
                point(3, List.of(0.0f, 0.0f, 0.9f, 0.1f), List.of(1, 5, 9), List.of(2.0f, 3.0f, 1.0f)),
                point(4, List.of(0.1f, 0.0f, 0.0f, 0.9f), List.of(1), List.of(1.0f))
            ))
            .build()
            .run(runContext);

        assertThat(upserted.getUpsertedCount(), is(4L));

        var fused = HybridQuery.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .denseVectorName(Property.ofValue("text_dense"))
            .denseVector(Property.ofValue(List.<Object>of(0.9f, 0.1f, 0.0f, 0.0f)))
            .sparseVectorName(Property.ofValue("text_sparse"))
            .sparseIndices(Property.ofValue(List.<Object>of(1, 5, 9)))
            .sparseValues(Property.ofValue(List.<Object>of(1.0f, 1.0f, 1.0f)))
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        var ids = fused.getRows().stream().map(row -> ((Map<?, ?>) row).get("id")).toList();
        assertThat(ids, is(List.<Object>of(1L, 3L, 4L, 2L)));
    }

    private static Map<String, Object> point(int id, List<Float> dense, List<Integer> indices, List<Float> values) {
        var vectors = new java.util.LinkedHashMap<String, Object>();
        vectors.put("text_dense", dense);
        if (indices != null) {
            vectors.put("text_sparse", Map.of("indices", indices, "values", values));
        }
        return Map.of("id", id, "vectors", vectors);
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
