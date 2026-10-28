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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HybridQueryTest extends QdrantTest {

    private static final String DENSE_VECTOR = "text_dense";
    private static final String SPARSE_VECTOR = "text_sparse";

    private static final List<Float> DENSE_QUERY = List.of(0.9f, 0.1f, 0.0f, 0.0f);
    private static final List<Integer> SPARSE_QUERY_INDICES = List.of(1, 5, 9);
    private static final List<Float> SPARSE_QUERY_VALUES = List.of(1.0f, 1.0f, 1.0f);

    /**
     * Fixture with deliberately divergent rankings, so fusion cannot be mistaken for either branch:
     * dense-only ranks 1, 2, 4, 3 and sparse-only ranks 3, 4, 1 (point 2 carries no sparse vector at all).
     */
    @BeforeEach
    void setUpCollection() throws Exception {
        if (!isQdrantAvailable()) return;

        var denseParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();

        try (var client = qdrantClient()) {
            client.createCollectionAsync(
                Collections.CreateCollection.newBuilder()
                    .setCollectionName(collectionName)
                    .setVectorsConfig(Collections.VectorsConfig.newBuilder()
                        .setParamsMap(Collections.VectorParamsMap.newBuilder().putMap(DENSE_VECTOR, denseParams).build())
                        .build())
                    .setSparseVectorsConfig(Collections.SparseVectorConfig.newBuilder()
                        .putMap(SPARSE_VECTOR, Collections.SparseVectorParams.newBuilder().build())
                        .build())
                    .build()
            ).get();

            var p1 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(1L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    DENSE_VECTOR, VectorFactory.vector(List.of(0.95f, 0.05f, 0.0f, 0.0f)),
                    SPARSE_VECTOR, VectorFactory.vector(List.of(0.1f), List.of(5))
                )))
                .putPayload("category", ValueFactory.value("docs"))
                .build();
            var p2 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(2L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    DENSE_VECTOR, VectorFactory.vector(List.of(0.6f, 0.4f, 0.0f, 0.0f))
                )))
                .putPayload("category", ValueFactory.value("docs"))
                .build();
            var p3 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(3L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    DENSE_VECTOR, VectorFactory.vector(List.of(0.0f, 0.0f, 0.9f, 0.1f)),
                    SPARSE_VECTOR, VectorFactory.vector(List.of(2.0f, 3.0f, 1.0f), List.of(1, 5, 9))
                )))
                .putPayload("category", ValueFactory.value("docs"))
                .build();
            var p4 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(4L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    DENSE_VECTOR, VectorFactory.vector(List.of(0.1f, 0.0f, 0.0f, 0.9f)),
                    SPARSE_VECTOR, VectorFactory.vector(List.of(1.0f), List.of(1))
                )))
                .putPayload("category", ValueFactory.value("archive"))
                .build();

            client.upsertAsync(collectionName, List.of(p1, p2, p3, p4)).get();
        }
    }

    @Test
    void fusedResultMatchesDirectSdkQuery() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = task()
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(4L));

        try (var client = qdrantClient()) {
            List<Points.ScoredPoint> expected = client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .addPrefetch(densePrefetch(10, null))
                    .addPrefetch(sparsePrefetch(10, null))
                    .setQuery(QueryFactory.fusion(Points.Fusion.RRF))
                    .setLimit(10)
                    .setWithPayload(WithPayloadSelectorFactory.enable(true))
                    .setWithVectors(WithVectorsSelectorFactory.enable(false))
                    .build()
            ).get();

            assertThat(rowIds(output.getRows()), is(pointIds(expected)));
            assertThat(rowScores(output.getRows()), is(pointScores(expected)));

            // the fused ranking must not be a copy of either branch
            List<Object> denseOnly = pointIds(client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .setQuery(QueryFactory.nearest(DENSE_QUERY))
                    .setUsing(DENSE_VECTOR)
                    .setLimit(10)
                    .build()
            ).get());
            List<Object> sparseOnly = pointIds(client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .setQuery(QueryFactory.nearest(SPARSE_QUERY_VALUES, SPARSE_QUERY_INDICES))
                    .setUsing(SPARSE_VECTOR)
                    .setLimit(10)
                    .build()
            ).get());

            assertThat(denseOnly, is(List.<Object>of(1L, 2L, 4L, 3L)));
            assertThat(sparseOnly, is(List.<Object>of(3L, 4L, 1L)));
            assertThat(rowIds(output.getRows()), is(not(denseOnly)));
            assertThat(rowIds(output.getRows()), is(not(sparseOnly)));
            assertThat(rowIds(output.getRows()), is(List.<Object>of(1L, 3L, 4L, 2L)));
        }
    }

    @Test
    void fusedResultHonoursLimitAndCandidateOverrides() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = task()
            .limit(Property.ofValue(3))
            .candidateLimit(Property.ofValue(10))
            .denseCandidateLimit(Property.ofValue(4))
            .sparseCandidateLimit(Property.ofValue(5))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(output.getSize(), is(3L));

        try (var client = qdrantClient()) {
            List<Points.ScoredPoint> expected = client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .addPrefetch(densePrefetch(4, null))
                    .addPrefetch(sparsePrefetch(5, null))
                    .setQuery(QueryFactory.fusion(Points.Fusion.RRF))
                    .setLimit(3)
                    .setWithPayload(WithPayloadSelectorFactory.enable(true))
                    .setWithVectors(WithVectorsSelectorFactory.enable(false))
                    .build()
            ).get();

            assertThat(rowIds(output.getRows()), is(pointIds(expected)));
            assertThat(rowScores(output.getRows()), is(pointScores(expected)));
        }
    }

    @Test
    void filterConstrainsBothBranchesAndFusedResult() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = task()
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .filter(Property.ofValue(Map.of("category", "docs")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        // point 4 is dense rank 3 and sparse rank 2, so dropping it proves both branches were filtered
        assertThat(output.getSize(), is(3L));
        assertThat(rowIds(output.getRows()), is(List.<Object>of(1L, 3L, 2L)));

        Common.Filter filter = Common.Filter.newBuilder()
            .addMust(ConditionFactory.matchKeyword("category", "docs"))
            .build();

        try (var client = qdrantClient()) {
            List<Points.ScoredPoint> expected = client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .addPrefetch(densePrefetch(10, filter))
                    .addPrefetch(sparsePrefetch(10, filter))
                    .setQuery(QueryFactory.fusion(Points.Fusion.RRF))
                    .setFilter(filter)
                    .setLimit(10)
                    .setWithPayload(WithPayloadSelectorFactory.enable(true))
                    .setWithVectors(WithVectorsSelectorFactory.enable(false))
                    .build()
            ).get();

            assertThat(rowIds(output.getRows()), is(pointIds(expected)));
            assertThat(rowScores(output.getRows()), is(pointScores(expected)));

            // each branch on its own also excludes point 4 under the same filter
            List<Object> filteredDense = pointIds(client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .setQuery(QueryFactory.nearest(DENSE_QUERY))
                    .setUsing(DENSE_VECTOR)
                    .setFilter(filter)
                    .setLimit(10)
                    .build()
            ).get());
            List<Object> filteredSparse = pointIds(client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .setQuery(QueryFactory.nearest(SPARSE_QUERY_VALUES, SPARSE_QUERY_INDICES))
                    .setUsing(SPARSE_VECTOR)
                    .setFilter(filter)
                    .setLimit(10)
                    .build()
            ).get());

            assertThat(filteredDense, is(List.<Object>of(1L, 2L, 3L)));
            assertThat(filteredSparse, is(List.<Object>of(3L, 1L)));

            // filtered fusion scores differ from unfiltered ones, because the excluded point changed the branch ranks
            List<Points.ScoredPoint> unfiltered = client.queryAsync(
                Points.QueryPoints.newBuilder()
                    .setCollectionName(collectionName)
                    .addPrefetch(densePrefetch(10, null))
                    .addPrefetch(sparsePrefetch(10, null))
                    .setQuery(QueryFactory.fusion(Points.Fusion.RRF))
                    .setLimit(10)
                    .build()
            ).get();

            assertThat(rowScores(output.getRows()), is(not(pointScores(unfiltered))));
        }
    }

    @Test
    void emptyResultIsValid() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var fetched = task()
            .filter(Property.ofValue(Map.of("category", "no_such_category")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(fetched.getSize(), is(0L));
        assertThat(fetched.getRows(), is(empty()));

        var fetchedOne = task()
            .filter(Property.ofValue(Map.of("category", "no_such_category")))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(fetchedOne.getSize(), is(0L));
        assertThat(fetchedOne.getRow(), nullValue());

        var stored = task()
            .filter(Property.ofValue(Map.of("category", "no_such_category")))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build()
            .run(runContext);

        assertThat(stored.getSize(), is(0L));
        assertThat(stored.getUri(), notNullValue());
        assertThat(readStoredIds(runContext, stored.getUri()), is(empty()));
    }

    @Test
    void allFetchModesPreserveTheResultShape() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var fetched = task()
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(fetched.getSize(), is(4L));
        assertThat(fetched.getRows().size(), is(4));
        assertThat(fetched.getUri(), nullValue());

        Map<?, ?> firstRow = (Map<?, ?>) fetched.getRows().getFirst();
        assertThat(firstRow.get("id"), is(1L));
        assertThat(firstRow.get("score"), notNullValue());
        assertThat(firstRow.get("version"), notNullValue());
        assertThat(((Map<?, ?>) firstRow.get("payload")).get("category"), is("docs"));
        assertThat(firstRow.containsKey("vector"), is(false));

        var fetchedOne = task()
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(fetchedOne.getSize(), is(1L));
        assertThat(fetchedOne.getRow().get("id"), is(1L));
        assertThat(fetchedOne.getRows(), nullValue());

        var withVectors = task()
            .limit(Property.ofValue(1))
            .candidateLimit(Property.ofValue(10))
            .withVectors(Property.ofValue(true))
            .withPayload(Property.ofValue(false))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(withVectors.getRow().get("vector"), notNullValue());
        assertThat(withVectors.getRow().containsKey("payload"), is(false));

        var stored = task()
            .limit(Property.ofValue(10))
            .candidateLimit(Property.ofValue(10))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build()
            .run(runContext);

        assertThat(stored.getSize(), is(4L));
        assertThat(stored.getUri(), notNullValue());
        assertThat(stored.getRows(), nullValue());
        assertThat(readStoredIds(runContext, stored.getUri()), is(List.<Object>of(1L, 3L, 4L, 2L)));
    }

    @Test
    void renderedPropertiesAreResolvedAtRuntime() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of(Map.of(
            "collection", collectionName,
            "dense", DENSE_QUERY,
            "indices", SPARSE_QUERY_INDICES,
            "values", SPARSE_QUERY_VALUES,
            "max", 2
        ));

        var output = HybridQuery.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofExpression("{{ collection }}"))
            .denseVectorName(Property.ofExpression("{{ 'text_' ~ 'dense' }}"))
            .denseVector(Property.ofExpression("{{ dense }}"))
            .sparseVectorName(Property.ofExpression("{{ 'text_' ~ 'sparse' }}"))
            .sparseIndices(Property.ofExpression("{{ indices }}"))
            .sparseValues(Property.ofExpression("{{ values }}"))
            .limit(Property.ofExpression("{{ max }}"))
            .candidateLimit(Property.ofExpression("{{ max * 5 }}"))
            .queryTimeout(Property.ofValue(Duration.ofSeconds(30)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(rowIds(output.getRows()), is(List.<Object>of(1L, 3L)));
    }

    @Test
    void missingVectorNameFailsOnTheServer() {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var denseTask = task()
            .denseVectorName(Property.ofValue("no_such_dense"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        var denseFailure = assertThrows(ExecutionException.class, () -> denseTask.run(runContext));
        assertThat(denseFailure.getMessage(), containsString("no_such_dense"));

        var sparseTask = task()
            .sparseVectorName(Property.ofValue("no_such_sparse"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        var sparseFailure = assertThrows(ExecutionException.class, () -> sparseTask.run(runContext));
        assertThat(sparseFailure.getMessage(), containsString("no_such_sparse"));
    }

    @Test
    void wrongDenseDimensionFailsOnTheServer() {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var task = task()
            .denseVector(Property.ofValue(List.of(0.9f, 0.1f)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var failure = assertThrows(ExecutionException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("dimension"));
    }

    @Test
    void emptyDenseVectorIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().denseVector(Property.ofValue(List.of())).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'denseVector' must contain at least one dimension"));
    }

    @Test
    void nonNumericDenseVectorIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().denseVector(Property.ofValue(List.of("not-a-number"))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'denseVector' must be a list of numbers"));
    }

    @Test
    void nonFiniteDenseVectorIsRejected() {
        var runContext = runContextFactory.of();

        var nanTask = task().denseVector(Property.ofValue(List.of(0.9f, Float.NaN, 0.0f, 0.0f))).build();
        var nanFailure = assertThrows(IllegalArgumentException.class, () -> nanTask.run(runContext));
        assertThat(nanFailure.getMessage(), containsString("'denseVector' must contain finite numbers only"));

        var infinityTask = task().denseVector(Property.ofValue(List.of(0.9f, Double.POSITIVE_INFINITY, 0.0f, 0.0f))).build();
        var infinityFailure = assertThrows(IllegalArgumentException.class, () -> infinityTask.run(runContext));
        assertThat(infinityFailure.getMessage(), containsString("'denseVector' must contain finite numbers only"));
    }

    @Test
    void emptySparseVectorIsRejected() {
        var runContext = runContextFactory.of();
        var task = task()
            .sparseIndices(Property.ofValue(List.of()))
            .sparseValues(Property.ofValue(List.of()))
            .build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("must contain at least one entry"));
    }

    @Test
    void mismatchedSparseLengthsAreRejected() {
        var runContext = runContextFactory.of();
        var task = task()
            .sparseIndices(Property.ofValue(List.of(1, 5, 9)))
            .sparseValues(Property.ofValue(List.of(1.0f, 1.0f)))
            .build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("must have the same number of elements, got: 3 indices and 2 values"));
    }

    @Test
    void negativeSparseIndexIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().sparseIndices(Property.ofValue(List.of(1, -5, 9))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'sparseIndices' must contain non-negative indices"));
    }

    @Test
    void duplicateSparseIndexIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().sparseIndices(Property.ofValue(List.of(1, 5, 5))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'sparseIndices' must not contain duplicate indices"));
    }

    @Test
    void fractionalSparseIndexIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().sparseIndices(Property.ofValue(List.of(1, 5.5, 9))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'sparseIndices' must contain whole numbers"));
    }

    @Test
    void nonFiniteSparseValueIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().sparseValues(Property.ofValue(List.of(1.0f, Float.NEGATIVE_INFINITY, 1.0f))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'sparseValues' must contain finite numbers only"));
    }

    @Test
    void outOfRangeLimitsAreRejected() {
        var runContext = runContextFactory.of();

        var zeroLimit = task().limit(Property.ofValue(0)).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> zeroLimit.run(runContext)).getMessage(),
            containsString("'limit' must be between 1 and 1000")
        );

        var hugeLimit = task().limit(Property.ofValue(1001)).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> hugeLimit.run(runContext)).getMessage(),
            containsString("'limit' must be between 1 and 1000")
        );

        var zeroCandidates = task().candidateLimit(Property.ofValue(0)).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> zeroCandidates.run(runContext)).getMessage(),
            containsString("'candidateLimit' must be between 1 and 10000")
        );

        var zeroDenseCandidates = task().denseCandidateLimit(Property.ofValue(0)).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> zeroDenseCandidates.run(runContext)).getMessage(),
            containsString("'denseCandidateLimit' must be between 1 and 10000")
        );

        var zeroSparseCandidates = task().sparseCandidateLimit(Property.ofValue(0)).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> zeroSparseCandidates.run(runContext)).getMessage(),
            containsString("'sparseCandidateLimit' must be between 1 and 10000")
        );
    }

    @Test
    void candidateLimitBelowFinalLimitIsRejected() {
        var runContext = runContextFactory.of();

        var sharedBudget = task()
            .limit(Property.ofValue(20))
            .candidateLimit(Property.ofValue(5))
            .build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> sharedBudget.run(runContext)).getMessage(),
            containsString("'candidateLimit' must be greater than or equal to 'limit'")
        );

        var denseBudget = task()
            .limit(Property.ofValue(20))
            .candidateLimit(Property.ofValue(20))
            .denseCandidateLimit(Property.ofValue(5))
            .build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> denseBudget.run(runContext)).getMessage(),
            containsString("'denseCandidateLimit' must be greater than or equal to 'limit'")
        );

        var sparseBudget = task()
            .limit(Property.ofValue(20))
            .candidateLimit(Property.ofValue(20))
            .sparseCandidateLimit(Property.ofValue(5))
            .build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> sparseBudget.run(runContext)).getMessage(),
            containsString("'sparseCandidateLimit' must be greater than or equal to 'limit'")
        );
    }

    @Test
    void subSecondQueryTimeoutIsRejected() {
        var runContext = runContextFactory.of();
        var task = task().queryTimeout(Property.ofValue(Duration.ofMillis(500))).build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(failure.getMessage(), containsString("'queryTimeout' must be at least 1 second"));
    }

    @Test
    void queryTimeoutDefaultsToThirtySeconds() throws Exception {
        var runContext = runContextFactory.of();
        var task = task().build();

        assertThat(runContext.render(task.getQueryTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofSeconds(30)));
    }

    @AfterEach
    void tearDown() {
        if (!isQdrantAvailable()) return;
        try (var client = qdrantClient()) {
            client.deleteCollectionAsync(collectionName).get();
        } catch (Exception ignored) {
        }
    }

    private HybridQuery.HybridQueryBuilder<?, ?> task() {
        return HybridQuery.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .denseVectorName(Property.ofValue(DENSE_VECTOR))
            .denseVector(Property.ofValue(List.<Object>copyOf(DENSE_QUERY)))
            .sparseVectorName(Property.ofValue(SPARSE_VECTOR))
            .sparseIndices(Property.ofValue(List.<Object>copyOf(SPARSE_QUERY_INDICES)))
            .sparseValues(Property.ofValue(List.<Object>copyOf(SPARSE_QUERY_VALUES)));
    }

    private static Points.PrefetchQuery densePrefetch(int candidateLimit, Common.Filter filter) {
        var builder = Points.PrefetchQuery.newBuilder()
            .setQuery(QueryFactory.nearest(DENSE_QUERY))
            .setUsing(DENSE_VECTOR)
            .setLimit(candidateLimit);
        if (filter != null) {
            builder.setFilter(filter);
        }
        return builder.build();
    }

    private static Points.PrefetchQuery sparsePrefetch(int candidateLimit, Common.Filter filter) {
        var builder = Points.PrefetchQuery.newBuilder()
            .setQuery(QueryFactory.nearest(SPARSE_QUERY_VALUES, SPARSE_QUERY_INDICES))
            .setUsing(SPARSE_VECTOR)
            .setLimit(candidateLimit);
        if (filter != null) {
            builder.setFilter(filter);
        }
        return builder.build();
    }

    private static List<Object> pointIds(List<Points.ScoredPoint> points) {
        return points.stream().map(point -> (Object) point.getId().getNum()).toList();
    }

    private static List<Object> pointScores(List<Points.ScoredPoint> points) {
        return points.stream().map(point -> (Object) point.getScore()).toList();
    }

    private static List<Object> rowIds(List<Object> rows) {
        return rows.stream().map(row -> (Object) ((Map<?, ?>) row).get("id")).toList();
    }

    private static List<Object> rowScores(List<Object> rows) {
        return rows.stream().map(row -> (Object) ((Map<?, ?>) row).get("score")).toList();
    }

    private static List<Object> readStoredIds(RunContext runContext, URI uri) throws Exception {
        List<Object> storedIds = new ArrayList<>();
        try (var reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(uri), StandardCharsets.UTF_8))) {
            FileSerde.readAll(reader)
                .doOnNext(row -> storedIds.add(((Number) ((Map<?, ?>) row).get("id")).longValue()))
                .blockLast();
        }
        return storedIds;
    }
}
