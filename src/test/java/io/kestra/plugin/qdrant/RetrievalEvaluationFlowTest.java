package io.kestra.plugin.qdrant;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.flows.State;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tenant.TenantService;
import io.qdrant.client.QueryFactory;
import io.qdrant.client.grpc.Points;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

// Runs the evaluation flow documented as an example of HybridQuery through the Kestra runner, against a real Qdrant.
@KestraTest(startRunner = true)
public class RetrievalEvaluationFlowTest extends QdrantTest {

    private static final String FLOW_ID = "qdrant_retrieval_evaluation";

    @Inject
    private FlowRepositoryInterface flowRepository;

    @Inject
    private TestRunnerUtils runnerUtils;

    @Test
    void documentedEvaluationFlowReportsTheSameHitsAsDirectSdkQueries() throws Exception {
        if (!isQdrantAvailable()) return;

        var flow = flowRepository.create(GenericFlow.fromYaml(TenantService.MAIN_TENANT, documentedFlow()));

        var execution = runnerUtils.runOne(flow.getTenantId(), flow.getNamespace(), flow.getId(), Duration.ofMinutes(2));

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        var summary = outputsOf(execution, "summary");
        var queries = defaultQueries(flow);

        // independent expectation, computed with the SDK on a second collection that mirrors the corpus
        var expected = expectedHits(queries, 3);
        assertThat(asInt(summary.get("queries")), is(queries.size()));
        assertThat(asInt(summary.get("dense_hits")), is(expected[0]));
        assertThat(asInt(summary.get("sparse_hits")), is(expected[1]));
        assertThat(asInt(summary.get("fused_hits")), is(expected[2]));

        // the example is only worth showing if the three methods really differ
        assertThat(expected[2], is(not(expected[0])));
        assertThat(expected[2], is(not(expected[1])));

        // the finally block dropped the throwaway collection
        try (var client = qdrantClient()) {
            var remaining = client.listCollectionsAsync().get();
            assertThat(remaining.stream().noneMatch(name -> name.startsWith("eval_")), is(true));
        }
    }

    @Test
    void theFlowDropsItsCollectionEvenWhenAQueryFails() throws Exception {
        if (!isQdrantAvailable()) return;

        // a sparse index that is not a number makes SparseQuery fail after the collection was created and filled
        var broken = documentedFlow().replace("id: " + FLOW_ID, "id: " + FLOW_ID + "_broken")
            .replace("\"indices\": [13]", "\"indices\": [\"x\"]");
        var flow = flowRepository.create(GenericFlow.fromYaml(TenantService.MAIN_TENANT, broken));

        var execution = runnerUtils.runOne(flow.getTenantId(), flow.getNamespace(), flow.getId(), Duration.ofMinutes(2));

        assertThat(execution.getState().getCurrent(), is(State.Type.FAILED));
        try (var client = qdrantClient()) {
            assertThat(client.listCollectionsAsync().get().stream().noneMatch(name -> name.startsWith("eval_")), is(true));
        }
    }

    // expected hit@k per method: {dense, sparse, fused}
    private int[] expectedHits(List<Map<String, Object>> queries, int k) throws Exception {
        var collection = collectionName + "_expected";
        try (var client = qdrantClient()) {
            CreateCollection.builder()
                .host(io.kestra.core.models.property.Property.ofValue(host))
                .port(io.kestra.core.models.property.Property.ofValue(port))
                .collectionName(io.kestra.core.models.property.Property.ofValue(collection))
                .vectorName(io.kestra.core.models.property.Property.ofValue("text_dense"))
                .vectorSize(io.kestra.core.models.property.Property.ofValue(4))
                .distance(io.kestra.core.models.property.Property.ofValue(Distance.COSINE))
                .sparseVectors(List.of(CreateCollection.SparseVectorSpace.builder()
                    .name(io.kestra.core.models.property.Property.ofValue("text_sparse")).build()))
                .build()
                .run(runContextFactory.of());

            Upsert.builder()
                .host(io.kestra.core.models.property.Property.ofValue(host))
                .port(io.kestra.core.models.property.Property.ofValue(port))
                .collectionName(io.kestra.core.models.property.Property.ofValue(collection))
                .points(corpus())
                .build()
                .run(runContextFactory.of());

            var hits = new int[3];
            for (var query : queries) {
                var dense = floats((List<?>) query.get("dense"));
                var indices = ints((List<?>) query.get("indices"));
                var values = floats((List<?>) query.get("values"));
                var relevant = ((Number) query.get("relevant")).longValue();

                var denseIds = ids(client.queryAsync(Points.QueryPoints.newBuilder().setCollectionName(collection)
                    .setQuery(QueryFactory.nearest(dense)).setUsing("text_dense").setLimit(k).build()).get());
                var sparseIds = ids(client.queryAsync(Points.QueryPoints.newBuilder().setCollectionName(collection)
                    .setQuery(QueryFactory.nearest(values, indices)).setUsing("text_sparse").setLimit(k).build()).get());
                var fusedIds = ids(client.queryAsync(Points.QueryPoints.newBuilder().setCollectionName(collection)
                    .addPrefetch(Points.PrefetchQuery.newBuilder().setQuery(QueryFactory.nearest(dense)).setUsing("text_dense").setLimit(6))
                    .addPrefetch(Points.PrefetchQuery.newBuilder().setQuery(QueryFactory.nearest(values, indices)).setUsing("text_sparse").setLimit(6))
                    .setQuery(QueryFactory.fusion(Points.Fusion.RRF)).setLimit(k).build()).get());

                hits[0] += denseIds.contains(relevant) ? 1 : 0;
                hits[1] += sparseIds.contains(relevant) ? 1 : 0;
                hits[2] += fusedIds.contains(relevant) ? 1 : 0;
            }
            client.deleteCollectionAsync(collection).get();
            return hits;
        }
    }

    // the corpus is read back from the documented flow so the expectation can never drift from the example
    private List<Map<String, Object>> corpus() throws Exception {
        var yaml = JacksonMapper.ofYaml().readValue(documentedFlow(), Map.class);
        @SuppressWarnings("unchecked")
        var tasks = (List<Map<String, Object>>) yaml.get("tasks");
        var load = tasks.stream().filter(task -> "load_corpus".equals(task.get("id"))).findFirst().orElseThrow();
        @SuppressWarnings("unchecked")
        var points = (List<Map<String, Object>>) load.get("points");
        return points;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> defaultQueries(io.kestra.core.models.flows.FlowInterface flow) throws Exception {
        var yaml = JacksonMapper.ofYaml().readValue(documentedFlow(), Map.class);
        var inputs = (List<Map<String, Object>>) yaml.get("inputs");
        var queries = inputs.stream().filter(input -> "queries".equals(input.get("id"))).findFirst().orElseThrow();
        return JacksonMapper.ofJson().readValue((String) queries.get("defaults"), List.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> outputsOf(Execution execution, String taskId) {
        var run = execution.getTaskRunList().stream().filter(taskRun -> taskId.equals(taskRun.getTaskId())).findFirst().orElseThrow();
        return (Map<String, Object>) run.getOutputs().get("values");
    }

    // swaps the secret based connection of the example for the test Qdrant, nothing else is touched
    private String documentedFlow() {
        var example = java.util.Arrays.stream(HybridQuery.class.getAnnotation(Plugin.class).examples())
            .map(annotation -> String.join("\n", annotation.code()))
            .filter(code -> code.contains("id: " + FLOW_ID))
            .findFirst()
            .orElseThrow();

        var lines = new ArrayList<String>();
        for (var line : example.split("\n")) {
            var trimmed = line.trim();
            var indent = line.substring(0, line.length() - line.stripLeading().length());
            if (trimmed.startsWith("host: ")) {
                lines.add(indent + "host: " + host);
                lines.add(indent + "port: " + port);
            } else if (trimmed.startsWith("apiKey: ") || trimmed.startsWith("tlsEnabled: ")) {
                continue;
            } else {
                lines.add(line);
            }
        }
        var flow = String.join("\n", lines);
        assertThat(flow, containsString("port: " + port));
        return flow;
    }

    // OutputValues renders every value through Pebble, so numbers come back as text
    private static int asInt(Object value) {
        return Integer.parseInt(String.valueOf(value).trim());
    }

    private static List<Long> ids(List<Points.ScoredPoint> points) {
        return points.stream().map(point -> point.getId().getNum()).toList();
    }

    private static List<Float> floats(List<?> values) {
        return values.stream().map(value -> ((Number) value).floatValue()).toList();
    }

    private static List<Integer> ints(List<?> values) {
        return values.stream().map(value -> ((Number) value).intValue()).toList();
    }
}
