package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.qdrant.client.ConditionFactory;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.QueryFactory;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.WithPayloadSelectorFactory;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SparseQueryTest extends QdrantTest {

    private static final String SPARSE_VECTOR = "text_sparse";

    private static final List<Integer> QUERY_INDICES = List.of(1, 5, 9);
    private static final List<Float> QUERY_VALUES = List.of(1.0f, 1.0f, 1.0f);

    // sparse-only ranking of this fixture is 3, 4, 1 (point 2 has no sparse vector, point 4 is archived)
    @BeforeEach
    void setUpCollection() throws Exception {
        if (!isQdrantAvailable()) return;

        try (var client = qdrantClient()) {
            client.createCollectionAsync(
                Collections.CreateCollection.newBuilder()
                    .setCollectionName(collectionName)
                    .setVectorsConfig(Collections.VectorsConfig.newBuilder()
                        .setParamsMap(Collections.VectorParamsMap.newBuilder().putMap(
                            "text_dense",
                            Collections.VectorParams.newBuilder().setSize(DIMENSION).setDistance(Collections.Distance.Cosine).build()
                        ).build()))
                    .setSparseVectorsConfig(Collections.SparseVectorConfig.newBuilder()
                        .putMap(SPARSE_VECTOR, Collections.SparseVectorParams.newBuilder().build()))
                    .build()
            ).get();

            client.upsertAsync(collectionName, List.of(
                point(1, "docs", List.of(0.1f), List.of(5)),
                point(2, "docs", null, null),
                point(3, "docs", List.of(2.0f, 3.0f, 1.0f), List.of(1, 5, 9)),
                point(4, "archive", List.of(1.0f), List.of(1))
            )).get();
        }
    }

    @Test
    void sparseResultMatchesDirectSdkQuery() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = task().fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContext);

        try (var client = qdrantClient()) {
            var expected = client.queryAsync(direct(10, null)).get();

            assertThat(rowIds(output.getRows()), is(ids(expected)));
            assertThat(rowScores(output.getRows()), is(scores(expected)));
            assertThat(rowIds(output.getRows()), is(List.<Object>of(3L, 4L, 1L)));
            assertThat(output.getSize(), is(3L));
        }
    }

    @Test
    void filterAndLimitConstrainTheResult() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var filtered = task()
            .filter(Property.ofValue(Map.of("category", "docs")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        var filter = Common.Filter.newBuilder().addMust(ConditionFactory.matchKeyword("category", "docs")).build();
        try (var client = qdrantClient()) {
            assertThat(rowIds(filtered.getRows()), is(ids(client.queryAsync(direct(10, filter)).get())));
        }
        assertThat(rowIds(filtered.getRows()), is(List.<Object>of(3L, 1L)));

        var limited = task().limit(Property.ofValue(1)).fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContext);
        assertThat(rowIds(limited.getRows()), is(List.<Object>of(3L)));
    }

    @Test
    void scoreThresholdDropsLowScoringPoints() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var all = task().fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContext);
        var scores = rowScores(all.getRows());
        var between = (((Number) scores.get(0)).floatValue() + ((Number) scores.get(1)).floatValue()) / 2;

        var thresholded = task()
            .scoreThreshold(Property.ofValue(between))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(rowIds(thresholded.getRows()), is(List.<Object>of(3L)));
    }

    @Test
    void emptyResultIsValid() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = task()
            .indices(Property.ofValue(List.<Object>of(777)))
            .values(Property.ofValue(List.<Object>of(1.0f)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(output.getRows(), is(empty()));
        assertThat(output.getSize(), is(0L));

        var one = task()
            .indices(Property.ofValue(List.<Object>of(777)))
            .values(Property.ofValue(List.<Object>of(1.0f)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);
        assertThat(one.getRow(), is(nullValue()));
    }

    @Test
    void allFetchModesPreserveTheResultShape() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var fetch = task().fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContext);
        var first = (Map<?, ?>) fetch.getRows().getFirst();
        assertThat(first.keySet().containsAll(List.of("id", "score", "version", "payload")), is(true));

        var fetchOne = task().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build().run(runContext);
        assertThat(((Map<?, ?>) fetchOne.getRow()).get("id"), is((Object) 3L));
        assertThat(fetchOne.getSize(), is(1L));

        var stored = task().fetchType(Property.ofValue(FetchType.STORE)).build().run(runContext);
        assertThat(stored.getUri(), is(notNullValue()));
        var storedIds = new ArrayList<Object>();
        try (var reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(stored.getUri()), StandardCharsets.UTF_8))) {
            FileSerde.readAll(reader)
                .doOnNext(row -> storedIds.add(((Number) ((Map<?, ?>) row).get("id")).longValue()))
                .blockLast();
        }
        assertThat(storedIds, is(List.<Object>of(3L, 4L, 1L)));
    }

    @Test
    void unknownSparseVectorNameFailsOnTheServer() {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();
        var task = task().vectorName(Property.ofValue("does_not_exist")).build();

        var failure = assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(rootMessage(failure), containsString("does_not_exist"));
    }

    @Test
    void invalidSparseInputIsRejectedBeforeAnyCall() {
        var runContext = runContextFactory.of();

        assertRejected(runContext, task().indices(Property.ofValue(List.<Object>of())).values(Property.ofValue(List.<Object>of())), "at least one entry");
        assertRejected(runContext, task().values(Property.ofValue(List.<Object>of(1.0f))), "must have the same number of elements");
        assertRejected(runContext, task().indices(Property.ofValue(List.<Object>of(1, 1, 5))), "must not contain duplicate indices");
        assertRejected(runContext, task().indices(Property.ofValue(List.<Object>of(-1, 5, 9))), "must contain non-negative indices");
        assertRejected(runContext, task().indices(Property.ofValue(List.<Object>of(1.5, 5, 9))), "must contain whole numbers");
        assertRejected(runContext, task().values(Property.ofValue(List.<Object>of(1.0f, Float.NaN, 1.0f))), "must contain finite numbers only");
        assertRejected(runContext, task().values(Property.ofValue(List.<Object>of("x", 1.0f, 1.0f))), "must be a list of numbers");
        assertRejected(runContext, task().limit(Property.ofValue(0)), "'limit' must be between 1 and 1000");
        assertRejected(runContext, task().limit(Property.ofValue(1001)), "'limit' must be between 1 and 1000");
        assertRejected(runContext, task().queryTimeout(Property.ofValue(Duration.ofMillis(500))), "'queryTimeout' must be at least 1 second");
    }

    @Test
    void renderedPropertiesAreResolvedAtRuntime() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of(Map.of("idx", List.of(1, 5, 9), "val", List.of(1.0, 1.0, 1.0), "max", 2));

        var output = task()
            .indices(Property.ofExpression("{{ idx }}"))
            .values(Property.ofExpression("{{ val }}"))
            .limit(Property.ofExpression("{{ max }}"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(rowIds(output.getRows()), is(List.<Object>of(3L, 4L)));
    }

    @Test
    void queryTimeoutDefaultsToThirtySeconds() throws Exception {
        var runContext = runContextFactory.of();

        assertThat(runContext.render(task().build().getQueryTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofSeconds(30)));
    }

    @AfterEach
    void tearDown() {
        if (!isQdrantAvailable()) return;
        try (var client = qdrantClient()) {
            client.deleteCollectionAsync(collectionName).get();
        } catch (Exception ignored) {
        }
    }

    private void assertRejected(RunContext runContext, SparseQuery.SparseQueryBuilder<?, ?> builder, String expected) {
        var task = builder.build();
        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString(expected));
    }

    private SparseQuery.SparseQueryBuilder<?, ?> task() {
        return SparseQuery.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vectorName(Property.ofValue(SPARSE_VECTOR))
            .indices(Property.ofValue(List.<Object>copyOf(QUERY_INDICES)))
            .values(Property.ofValue(List.<Object>copyOf(QUERY_VALUES)));
    }

    private Points.QueryPoints direct(int limit, Common.Filter filter) {
        var builder = Points.QueryPoints.newBuilder()
            .setCollectionName(collectionName)
            .setQuery(QueryFactory.nearest(QUERY_VALUES, QUERY_INDICES))
            .setUsing(SPARSE_VECTOR)
            .setLimit(limit)
            .setWithPayload(WithPayloadSelectorFactory.enable(true))
            .setWithVectors(WithVectorsSelectorFactory.enable(false));
        if (filter != null) {
            builder.setFilter(filter);
        }
        return builder.build();
    }

    private static Points.PointStruct point(long id, String category, List<Float> values, List<Integer> indices) {
        var vectors = new java.util.LinkedHashMap<String, Points.Vector>();
        vectors.put("text_dense", VectorFactory.vector(List.of(1.0f, 0.0f, 0.0f, 0.0f)));
        if (values != null) {
            vectors.put(SPARSE_VECTOR, VectorFactory.vector(values, indices));
        }
        return Points.PointStruct.newBuilder()
            .setId(PointIdFactory.id(id))
            .setVectors(VectorsFactory.namedVectors(vectors))
            .putPayload("category", ValueFactory.value(category))
            .build();
    }

    private static List<Object> ids(List<Points.ScoredPoint> points) {
        return points.stream().map(point -> (Object) point.getId().getNum()).toList();
    }

    private static List<Object> scores(List<Points.ScoredPoint> points) {
        return points.stream().map(point -> (Object) point.getScore()).toList();
    }

    private static List<Object> rowIds(List<Object> rows) {
        return rows.stream().map(row -> (Object) ((Map<?, ?>) row).get("id")).toList();
    }

    private static List<Object> rowScores(List<Object> rows) {
        return rows.stream().map(row -> (Object) ((Map<?, ?>) row).get("score")).toList();
    }

    private static String rootMessage(Throwable throwable) {
        var message = new StringBuilder(String.valueOf(throwable.getMessage()));
        var current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
            message.append(" | ").append(current.getMessage());
        }
        return message.toString();
    }
}
