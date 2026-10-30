package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.serializers.YamlParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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

    @Test
    void theDocumentedCreateAndUpsertExamplesChain() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        // the two examples are meant to be run one after the other on the same collection
        var create = (CreateCollection) taskOf(CreateCollection.class, "qdrant_create_hybrid_collection");
        var upsert = (Upsert) taskOf(Upsert.class, "qdrant_upsert_hybrid");

        assertThat(create.run(runContext).getSuccess(), is(true));
        assertThat(upsert.run(runContext).getUpsertedCount(), is(1L));
    }

    // takes the task of a documented example, pointed at the test Qdrant and the collection of this test
    private Task taskOf(Class<? extends Task> type, String flowId) {
        var example = Arrays.stream(type.getAnnotation(Plugin.class).examples())
            .map(annotation -> String.join("\n", annotation.code()))
            .filter(code -> code.contains("id: " + flowId))
            .findFirst()
            .orElseThrow();

        var lines = new ArrayList<String>();
        for (var line : example.split("\n")) {
            var trimmed = line.trim();
            var indent = line.substring(0, line.length() - line.stripLeading().length());
            if (trimmed.startsWith("host: ")) {
                lines.add(indent + "host: " + host);
                lines.add(indent + "port: " + port);
            } else if (trimmed.startsWith("collectionName: ")) {
                lines.add(indent + "collectionName: " + collectionName);
            } else if (!trimmed.startsWith("apiKey: ") && !trimmed.startsWith("tlsEnabled: ")) {
                lines.add(line);
            }
        }
        return YamlParser.parse(String.join("\n", lines), Flow.class).getTasks().getFirst();
    }

    private static Map<String, Object> point(int id, List<Float> dense, List<Integer> indices, List<Float> values) {
        var vectors = new LinkedHashMap<String, Object>();
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
