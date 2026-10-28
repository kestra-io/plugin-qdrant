package io.kestra.plugin.qdrant;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.QueryFactory;
import io.qdrant.client.WithPayloadSelectorFactory;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Qdrant collection with dense and sparse vectors fused by RRF",
    description = """
        Runs one server-side hybrid query against a collection that holds both a named dense vector space and a named sparse vector space.

        The task sends a single `QueryPoints` request with two prefetch branches, one per vector space, each bounded by its own candidate limit, and asks Qdrant to merge the two candidate lists with native Reciprocal Rank Fusion by default, or Distribution-Based Score Fusion with `fusion: DBSF`. Fusion happens inside Qdrant, so no scoring is done by Kestra.

        The collection must already expose the dense and the sparse vector space under the names given here. Create it with the `CreateCollection` task (`vectorName` and `sparseVectors`) and load it with the `Upsert` task, which accepts sparse vectors. Hybrid retrieval is opt-in: the existing `Query` task is unchanged, and fusion does not improve every workload, so compare it with dense-only retrieval on your own labelled queries before adopting it."""
)
@Plugin(
    examples = {
        @Example(
            title = "Fuse a dense branch and a sparse branch with server-side RRF, filtering both branches and the fused result. The `docs` collection must already hold points with a `text_dense` dense vector and a `text_sparse` sparse vector.",
            full = true,
            code = """
                id: qdrant_hybrid_query
                namespace: company.team

                inputs:
                  - id: dense_embedding
                    type: ARRAY
                    itemType: FLOAT
                    defaults: [0.9, 0.1, 0.0, 0.0]

                tasks:
                  - id: hybrid_search
                    type: io.kestra.plugin.qdrant.HybridQuery
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    denseVectorName: text_dense
                    denseVector: "{{ inputs.dense_embedding }}"
                    sparseVectorName: text_sparse
                    sparseIndices: [12, 104, 982]
                    sparseValues: [0.84, 1.23, 0.41]
                    candidateLimit: 50
                    limit: 10
                    filter:
                      tenant_id: acme
                    withPayload: true
                    fetchType: FETCH

                  - id: log_hits
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.hybrid_search.rows }}"
                """
        ),
        @Example(
            title = "Tune the fusion: weight the sparse branch higher in RRF (needs Qdrant 1.17 or later), or switch to DBSF, and read the second page of results",
            full = true,
            code = """
                id: qdrant_hybrid_query_tuned
                namespace: company.team

                tasks:
                  - id: rrf_sparse_heavy
                    type: io.kestra.plugin.qdrant.HybridQuery
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    denseVectorName: text_dense
                    denseVector: [0.9, 0.1, 0.0, 0.0]
                    sparseVectorName: text_sparse
                    sparseIndices: [12, 104, 982]
                    sparseValues: [0.84, 1.23, 0.41]
                    rrfK: 5
                    denseWeight: 1.0
                    sparseWeight: 2.0
                    limit: 10
                    fetchType: FETCH

                  - id: dbsf_second_page
                    type: io.kestra.plugin.qdrant.HybridQuery
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    denseVectorName: text_dense
                    denseVector: [0.9, 0.1, 0.0, 0.0]
                    sparseVectorName: text_sparse
                    sparseIndices: [12, 104, 982]
                    sparseValues: [0.84, 1.23, 0.41]
                    fusion: DBSF
                    limit: 10
                    offset: 10
                    candidateLimit: 50
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Evaluate dense-only, sparse-only and fused retrieval on a labelled query set. The flow creates a throwaway collection, loads a small corpus, scores hit@k per method and always drops the collection.",
            full = true,
            code = """
                id: qdrant_retrieval_evaluation
                namespace: company.team

                variables:
                  collection: "eval_{{ execution.id | lower }}"

                inputs:
                  - id: k
                    type: INT
                    defaults: 3
                    description: A query counts as a hit when its relevant point is among the first k results of a method.
                  - id: queries
                    type: JSON
                    description: Labelled queries. Each one has a dense vector, a sparse vector and the id of the point that is relevant for it.
                    defaults: |
                      [
                        {"name": "exact identifier", "dense": [0.0, 0.3, 0.7, 0.0], "indices": [13], "values": [1.0], "relevant": 4},
                        {"name": "paraphrase", "dense": [0.1, 0.9, 0.0, 0.0], "indices": [99], "values": [1.0], "relevant": 3},
                        {"name": "both signals", "dense": [0.9, 0.1, 0.0, 0.0], "indices": [10], "values": [1.0], "relevant": 1},
                        {"name": "keyword only", "dense": [0.25, 0.25, 0.25, 0.25], "indices": [14], "values": [1.0], "relevant": 5}
                      ]

                tasks:
                  - id: create_collection
                    type: io.kestra.plugin.qdrant.CreateCollection
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: "{{ render(vars.collection) }}"
                    vectorName: text_dense
                    vectorSize: 4
                    distance: COSINE
                    sparseVectors:
                      - name: text_sparse

                  - id: load_corpus
                    type: io.kestra.plugin.qdrant.Upsert
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: "{{ render(vars.collection) }}"
                    points:
                      - id: 1
                        vectors:
                          text_dense: [1.0, 0.0, 0.0, 0.0]
                          text_sparse: {indices: [10, 20], values: [1.0, 0.5]}
                      - id: 2
                        vectors:
                          text_dense: [0.9, 0.1, 0.0, 0.0]
                          text_sparse: {indices: [11], values: [1.0]}
                      - id: 3
                        vectors:
                          text_dense: [0.0, 1.0, 0.0, 0.0]
                          text_sparse: {indices: [12, 20], values: [1.0, 0.5]}
                      - id: 4
                        vectors:
                          text_dense: [0.0, 0.1, 0.9, 0.0]
                          text_sparse: {indices: [13], values: [2.0]}
                      - id: 5
                        vectors:
                          text_dense: [0.0, 0.0, 1.0, 0.0]
                          text_sparse: {indices: [14], values: [1.0]}
                      - id: 6
                        vectors:
                          text_dense: [0.0, 0.0, 0.1, 0.9]
                          text_sparse: {indices: [15, 13], values: [1.0, 0.2]}

                  - id: evaluate
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ inputs.queries }}"
                    tasks:
                      - id: dense_only
                        type: io.kestra.plugin.qdrant.Query
                        host: "{{ secret('QDRANT_HOST') }}"
                        apiKey: "{{ secret('QDRANT_API_KEY') }}"
                        tlsEnabled: true
                        collectionName: "{{ render(vars.collection) }}"
                        vectorName: text_dense
                        vector: "{{ fromJson(taskrun.value).dense }}"
                        limit: "{{ inputs.k }}"
                        fetchType: FETCH

                      - id: sparse_only
                        type: io.kestra.plugin.qdrant.SparseQuery
                        host: "{{ secret('QDRANT_HOST') }}"
                        apiKey: "{{ secret('QDRANT_API_KEY') }}"
                        tlsEnabled: true
                        collectionName: "{{ render(vars.collection) }}"
                        vectorName: text_sparse
                        indices: "{{ fromJson(taskrun.value).indices }}"
                        values: "{{ fromJson(taskrun.value).values }}"
                        limit: "{{ inputs.k }}"
                        fetchType: FETCH

                      - id: fused
                        type: io.kestra.plugin.qdrant.HybridQuery
                        host: "{{ secret('QDRANT_HOST') }}"
                        apiKey: "{{ secret('QDRANT_API_KEY') }}"
                        tlsEnabled: true
                        collectionName: "{{ render(vars.collection) }}"
                        denseVectorName: text_dense
                        denseVector: "{{ fromJson(taskrun.value).dense }}"
                        sparseVectorName: text_sparse
                        sparseIndices: "{{ fromJson(taskrun.value).indices }}"
                        sparseValues: "{{ fromJson(taskrun.value).values }}"
                        limit: "{{ inputs.k }}"
                        candidateLimit: 6
                        fetchType: FETCH

                      - id: score
                        type: io.kestra.plugin.core.output.OutputValues
                        values:
                          name: "{{ fromJson(taskrun.value).name }}"
                          dense_hit: "{{ outputs.dense_only[taskrun.value].rows | jq('if (map(.id) | index(' ~ fromJson(taskrun.value).relevant ~ ')) != null then 1 else 0 end') | first }}"
                          sparse_hit: "{{ outputs.sparse_only[taskrun.value].rows | jq('if (map(.id) | index(' ~ fromJson(taskrun.value).relevant ~ ')) != null then 1 else 0 end') | first }}"
                          fused_hit: "{{ outputs.fused[taskrun.value].rows | jq('if (map(.id) | index(' ~ fromJson(taskrun.value).relevant ~ ')) != null then 1 else 0 end') | first }}"

                  - id: summary
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      queries: "{{ outputs.score | jq('length') | first }}"
                      dense_hits: "{{ outputs.score | jq('[.[].values.dense_hit | tonumber] | add') | first }}"
                      sparse_hits: "{{ outputs.score | jq('[.[].values.sparse_hit | tonumber] | add') | first }}"
                      fused_hits: "{{ outputs.score | jq('[.[].values.fused_hit | tonumber] | add') | first }}"

                  - id: report
                    type: io.kestra.plugin.core.log.Log
                    message: |
                      hit@{{ inputs.k }} on {{ outputs.summary.values.queries }} labelled queries
                      dense only : {{ outputs.summary.values.dense_hits }}
                      sparse only: {{ outputs.summary.values.sparse_hits }}
                      fused (RRF): {{ outputs.summary.values.fused_hits }}

                finally:
                  - id: drop_collection
                    type: io.kestra.plugin.qdrant.DeleteCollection
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: "{{ render(vars.collection) }}"
                """
        ),
        @Example(
            title = "Compare dense-only retrieval with fused retrieval on the same query, then store the fused ranking",
            full = true,
            code = """
                id: qdrant_hybrid_query_comparison
                namespace: company.team

                tasks:
                  - id: dense_only
                    type: io.kestra.plugin.qdrant.Query
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vectorName: text_dense
                    vector: [0.9, 0.1, 0.0, 0.0]
                    limit: 10
                    fetchType: FETCH

                  - id: hybrid
                    type: io.kestra.plugin.qdrant.HybridQuery
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    denseVectorName: text_dense
                    denseVector: [0.9, 0.1, 0.0, 0.0]
                    sparseVectorName: text_sparse
                    sparseIndices: [12, 104, 982]
                    sparseValues: [0.84, 1.23, 0.41]
                    denseCandidateLimit: 100
                    sparseCandidateLimit: 100
                    limit: 10
                    queryTimeout: PT10S
                    fetchType: STORE

                  - id: compare
                    type: io.kestra.plugin.core.log.Log
                    message: |
                      dense-only ids: {{ outputs.dense_only[taskrun.value].rows | jq('[.[].id]') }}
                      fused ranking stored at: {{ outputs.hybrid.uri }}
                """
        )
    }
)
public class HybridQuery extends QdrantConnection implements RunnableTask<FetchOutput> {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_CANDIDATE_LIMIT = 50;
    private static final int MAX_LIMIT = 1000;
    private static final int MAX_CANDIDATE_LIMIT = 10000;
    private static final int MAX_OFFSET = 10000;
    private static final int MAX_RRF_K = 10000;
    private static final float MAX_RRF_WEIGHT = 1000f;
    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(30);

    @Schema(
        title = "Collection name",
        description = """
            The name of the collection to search in. It must expose both the dense and the sparse vector space \
            named below."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Dense vector space name",
        description = """
            Name of the dense vector space to use for the dense prefetch branch, as configured on the \
            collection."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> denseVectorName;

    @Schema(
        title = "Dense query vector",
        description = """
            Dense query embedding, as a list of finite numbers. Its dimension must match the dense vector space \
            of the collection."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> denseVector;

    @Schema(
        title = "Sparse vector space name",
        description = """
            Name of the sparse vector space to use for the sparse prefetch branch, as configured on the \
            collection."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sparseVectorName;

    @Schema(
        title = "Sparse query indices",
        description = """
            Indices of the non-zero dimensions of the sparse query vector, as a list of distinct non-negative \
            whole numbers. Must have the same number of elements as 'sparseValues'."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> sparseIndices;

    @Schema(
        title = "Sparse query values",
        description = """
            Weights of the non-zero dimensions of the sparse query vector, as a list of finite numbers \
            positionally paired with 'sparseIndices'."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> sparseValues;

    @Schema(
        title = "Candidate limit per branch",
        description = """
            Maximum number of candidates each prefetch branch returns to the fusion step (default: 50, min: 1, \
            max: 10000). Must be greater than or equal to 'limit' plus 'offset'. Overridden per branch by \
            'denseCandidateLimit' and 'sparseCandidateLimit'."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> candidateLimit = Property.ofValue(DEFAULT_CANDIDATE_LIMIT);

    @Schema(
        title = "Candidate limit of the dense branch",
        description = """
            Overrides 'candidateLimit' for the dense prefetch branch only (min: 1, max: 10000). Must be greater \
            than or equal to 'limit'."""
    )
    @PluginProperty(group = "processing")
    private Property<Integer> denseCandidateLimit;

    @Schema(
        title = "Candidate limit of the sparse branch",
        description = """
            Overrides 'candidateLimit' for the sparse prefetch branch only (min: 1, max: 10000). Must be greater \
            than or equal to 'limit'."""
    )
    @PluginProperty(group = "processing")
    private Property<Integer> sparseCandidateLimit;

    @Schema(
        title = "Limit fused results",
        description = """
            Maximum number of points returned after fusion (default: 10, min: 1, max: 1000)."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> limit = Property.ofValue(DEFAULT_LIMIT);

    @Schema(
        title = "Skip fused results",
        description = """
            Number of fused results to skip before returning `limit` points, for pagination (default: 0, max: 10000). \
            Every candidate limit must be at least `limit + offset`, because fusion has to rank the skipped points too."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> offset = Property.ofValue(0);

    @Schema(
        title = "Fusion method",
        description = """
            How the two candidate lists are merged: `RRF` (default) scores a point from its rank in each branch, \
            `DBSF` normalizes each branch score distribution and sums the normalized scores. The returned `score` is \
            the fusion score for either method."""
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "processing")
    private Property<FusionMethod> fusion = Property.ofValue(FusionMethod.RRF);

    @Schema(
        title = "RRF rank constant",
        description = """
            The `k` constant of Reciprocal Rank Fusion (min: 1, max: 10000). With equal weights a point at \
            zero-based position `pos` in a branch scores `1 / (pos + k)`, so a larger `k` flattens the gap between \
            top ranks. Unset keeps the Qdrant default of 2. Use 61 to reproduce the `1 / (rank + 60)` convention of \
            the original RRF paper. Only valid with `fusion: RRF`, and needs Qdrant 1.17 or later."""
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> rrfK;

    @Schema(
        title = "RRF weight of the dense branch",
        description = """
            Weight of the dense branch in Reciprocal Rank Fusion (min: 0, max: 1000). Weights are absolute, not \
            relative: `1` and `2` is a different setting from `2` and `4`, because the position is divided by the \
            weight. A weight of `0` keeps the branch candidates at the bottom with a score of 0. Unset keeps the \
            Qdrant default of 1. Only valid with `fusion: RRF`, must be set together with `sparseWeight`, and needs \
            Qdrant 1.17 or later."""
    )
    @PluginProperty(group = "advanced")
    private Property<Float> denseWeight;

    @Schema(
        title = "RRF weight of the sparse branch",
        description = """
            Weight of the sparse branch in Reciprocal Rank Fusion (min: 0, max: 1000). Weights are absolute, not \
            relative, as described for `denseWeight`. Unset keeps the Qdrant default of 1. Only valid with \
            `fusion: RRF`, must be set together with `denseWeight`, and needs Qdrant 1.17 or later."""
    )
    @PluginProperty(group = "advanced")
    private Property<Float> sparseWeight;

    @Schema(
        title = "Filter criteria",
        description = """
            Filter map applied to both prefetch branches and to the fused result, so a point excluded by the \
            filter cannot enter either candidate list. Accepts the same filter shapes as the 'Query' task."""
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Include payload",
        description = """
            Whether to include point payload metadata in the result (default: true)."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> withPayload = Property.ofValue(true);

    @Schema(
        title = "Include vectors",
        description = """
            Whether to include vector embeddings in the result (default: false)."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> withVectors = Property.ofValue(false);

    @Schema(
        title = "Query timeout",
        description = """
            Server-side and client-side bound for the hybrid query, truncated to whole seconds (default: PT30S, \
            min: 1 second). The same value is sent as the Qdrant request timeout and used as the gRPC call \
            deadline."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Duration> queryTimeout = Property.ofValue(DEFAULT_QUERY_TIMEOUT);

    @Schema(
        title = "Select fetch behavior",
        description = """
            Output mode for query results: FETCH returns all rows inline, FETCH_ONE returns the first row, and \
            STORE writes all rows to Kestra storage as an ION file and returns the URI (default: STORE)."""
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        var rDenseVectorName = runContext.render(this.denseVectorName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'denseVectorName' is required"));
        var rSparseVectorName = runContext.render(this.sparseVectorName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'sparseVectorName' is required"));

        var rDenseVector = toFiniteFloats(renderList(runContext, this.denseVector, "denseVector"), "denseVector");
        if (rDenseVector.isEmpty()) {
            throw new IllegalArgumentException("'denseVector' must contain at least one dimension");
        }

        var rSparseIndices = toSparseIndices(renderList(runContext, this.sparseIndices, "sparseIndices"), "sparseIndices");
        var rSparseValues = toFiniteFloats(renderList(runContext, this.sparseValues, "sparseValues"), "sparseValues");
        if (rSparseIndices.isEmpty()) {
            throw new IllegalArgumentException("'sparseIndices' and 'sparseValues' must contain at least one entry");
        }
        if (rSparseIndices.size() != rSparseValues.size()) {
            throw new IllegalArgumentException(
                "'sparseIndices' and 'sparseValues' must have the same number of elements, got: "
                    + rSparseIndices.size() + " indices and " + rSparseValues.size() + " values"
            );
        }

        var rLimit = renderBoundedLimit(runContext, this.limit, "limit", DEFAULT_LIMIT, MAX_LIMIT);
        var rCandidateLimit = renderBoundedLimit(runContext, this.candidateLimit, "candidateLimit", DEFAULT_CANDIDATE_LIMIT, MAX_CANDIDATE_LIMIT);
        var rDenseCandidateLimit = renderBoundedLimit(runContext, this.denseCandidateLimit, "denseCandidateLimit", rCandidateLimit, MAX_CANDIDATE_LIMIT);
        var rSparseCandidateLimit = renderBoundedLimit(runContext, this.sparseCandidateLimit, "sparseCandidateLimit", rCandidateLimit, MAX_CANDIDATE_LIMIT);

        var rOffset = renderBoundedOffset(runContext, this.offset);
        var rWindow = rLimit + rOffset;

        requireCandidateBudget("candidateLimit", rCandidateLimit, rWindow);
        requireCandidateBudget("denseCandidateLimit", rDenseCandidateLimit, rWindow);
        requireCandidateBudget("sparseCandidateLimit", rSparseCandidateLimit, rWindow);

        var rFusion = runContext.render(this.fusion).as(FusionMethod.class).orElse(FusionMethod.RRF);
        var fusionQuery = fusionQuery(runContext, rFusion);

        var rWithPayload = runContext.render(this.withPayload).as(Boolean.class).orElse(true);
        var rWithVectors = runContext.render(this.withVectors).as(Boolean.class).orElse(false);
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);

        var rQueryTimeout = runContext.render(this.queryTimeout).as(Duration.class).orElse(DEFAULT_QUERY_TIMEOUT);
        var rTimeoutSeconds = rQueryTimeout.toSeconds();
        if (rTimeoutSeconds < 1) {
            throw new IllegalArgumentException("'queryTimeout' must be at least 1 second, got: " + rQueryTimeout);
        }

        Common.Filter rFilter = null;
        if (this.filter != null) {
            var rFilterMap = runContext.render(this.filter).asMap(String.class, Object.class);
            rFilter = toFilter(rFilterMap);
        }

        var densePrefetch = prefetch(
            QueryFactory.nearest(rDenseVector),
            rDenseVectorName,
            rDenseCandidateLimit,
            rFilter
        );
        var sparsePrefetch = prefetch(
            QueryFactory.nearest(rSparseValues, rSparseIndices),
            rSparseVectorName,
            rSparseCandidateLimit,
            rFilter
        );

        var queryBuilder = Points.QueryPoints.newBuilder()
            .setCollectionName(rCollectionName)
            .addPrefetch(densePrefetch)
            .addPrefetch(sparsePrefetch)
            .setQuery(fusionQuery)
            .setLimit(rLimit)
            .setOffset(rOffset)
            .setTimeout(rTimeoutSeconds)
            .setWithPayload(WithPayloadSelectorFactory.enable(rWithPayload))
            .setWithVectors(WithVectorsSelectorFactory.enable(rWithVectors));

        if (rFilter != null) {
            queryBuilder.setFilter(rFilter);
        }

        try (var client = buildClient(runContext)) {
            runContext.logger().info(
                "Hybrid querying collection '{}' with dense branch '{}' (candidates={}), sparse branch '{}' (candidates={}), {} fusion, limit={}, offset={} and timeout={}",
                rCollectionName, rDenseVectorName, rDenseCandidateLimit, rSparseVectorName, rSparseCandidateLimit, rFusion, rLimit, rOffset, rQueryTimeout
            );

            var results = client.queryAsync(queryBuilder.build(), rQueryTimeout).get();

            return buildFetchOutput(runContext, rFetchType, results, HybridQuery::mapScoredPoint);
        }
    }

    private static Points.PrefetchQuery prefetch(Points.Query query, String using, int candidateLimit, Common.Filter filter) {
        var builder = Points.PrefetchQuery.newBuilder()
            .setQuery(query)
            .setUsing(using)
            .setLimit(candidateLimit);
        if (filter != null) {
            builder.setFilter(filter);
        }
        return builder.build();
    }

    private static int renderBoundedLimit(RunContext runContext, Property<Integer> property, String name, int defaultValue, int max)
        throws IllegalVariableEvaluationException {
        if (property == null) {
            return defaultValue;
        }
        var rLimit = runContext.render(property).as(Integer.class).orElse(defaultValue);
        validateRange(name, rLimit, 1, max);
        return rLimit;
    }

    private static int renderBoundedOffset(RunContext runContext, Property<Integer> property)
        throws IllegalVariableEvaluationException {
        if (property == null) {
            return 0;
        }
        var rOffset = runContext.render(property).as(Integer.class).orElse(0);
        validateRange("offset", rOffset, 0, MAX_OFFSET);
        return rOffset;
    }

    private static void requireCandidateBudget(String name, int candidateLimit, int window) {
        if (candidateLimit < window) {
            throw new IllegalArgumentException(
                "'" + name + "' must be greater than or equal to 'limit' plus 'offset' so fusion has enough candidates, got: "
                    + candidateLimit + " candidates for a window of " + window
            );
        }
    }

    private Points.Query fusionQuery(RunContext runContext, FusionMethod rFusion) throws IllegalVariableEvaluationException {
        var rRrfK = this.rrfK == null ? null : runContext.render(this.rrfK).as(Integer.class).orElse(null);
        var rDenseWeight = this.denseWeight == null ? null : runContext.render(this.denseWeight).as(Float.class).orElse(null);
        var rSparseWeight = this.sparseWeight == null ? null : runContext.render(this.sparseWeight).as(Float.class).orElse(null);
        var tuned = rRrfK != null || rDenseWeight != null || rSparseWeight != null;

        if (rFusion == FusionMethod.DBSF) {
            if (tuned) {
                throw new IllegalArgumentException("'rrfK', 'denseWeight' and 'sparseWeight' only apply to 'fusion: RRF', but 'fusion' is DBSF");
            }
            return QueryFactory.fusion(rFusion.toGrpc());
        }
        if (!tuned) {
            return QueryFactory.fusion(rFusion.toGrpc());
        }
        if ((rDenseWeight == null) != (rSparseWeight == null)) {
            throw new IllegalArgumentException("'denseWeight' and 'sparseWeight' must be set together, because Qdrant takes one weight per branch");
        }

        var rrf = Points.Rrf.newBuilder();
        if (rRrfK != null) {
            validateRange("rrfK", rRrfK, 1, MAX_RRF_K);
            rrf.setK(rRrfK);
        }
        if (rDenseWeight != null) {
            // weights are positional: the dense prefetch is added first, the sparse prefetch second
            rrf.addWeights(requireWeight("denseWeight", rDenseWeight));
            rrf.addWeights(requireWeight("sparseWeight", rSparseWeight));
        }
        return QueryFactory.rrf(rrf.build());
    }

    private static float requireWeight(String name, float weight) {
        if (Float.isNaN(weight) || Float.isInfinite(weight) || weight < 0 || weight > MAX_RRF_WEIGHT) {
            throw new IllegalArgumentException("'" + name + "' must be a finite number between 0 and " + (int) MAX_RRF_WEIGHT + ", got: " + weight);
        }
        return weight;
    }
}
