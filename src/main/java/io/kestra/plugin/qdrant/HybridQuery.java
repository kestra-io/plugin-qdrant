package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Qdrant collection with dense and sparse vectors fused by RRF",
    description = """
        Runs one server-side hybrid query against a collection that holds both a named dense vector space and a named sparse vector space.

        The task sends a single `QueryPoints` request with two prefetch branches, one per vector space, each bounded by its own candidate limit, and asks Qdrant to merge the two candidate lists with native Reciprocal Rank Fusion. Fusion happens inside Qdrant, so no scoring is done by Kestra.

        The collection must already expose the dense and the sparse vector space under the names given here. Creating a sparse vector space and upserting sparse vectors are not part of this task. Hybrid retrieval is opt-in: the existing `Query` task is unchanged, and fusion does not improve every workload, so compare it with dense-only retrieval on your own labelled queries before adopting it."""
)
@Plugin(
    examples = {
        @Example(
            title = "Fuse a dense branch and a sparse branch with server-side RRF, filtering both branches and the fused result",
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
                  - id: prepare_collection
                    type: io.kestra.plugin.core.http.Request
                    uri: "https://{{ secret('QDRANT_HOST') }}:6333/collections/docs"
                    method: PUT
                    contentType: application/json
                    headers:
                      api-key: "{{ secret('QDRANT_API_KEY') }}"
                    body: |
                      {
                        "vectors": {"text_dense": {"size": 4, "distance": "Cosine"}},
                        "sparse_vectors": {"text_sparse": {}}
                      }

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
                      dense-only ids: {{ outputs.dense_only.rows | jq('[.[].id]') }}
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

    @Schema(
        title = "Collection name",
        description = "The name of the collection to search in. It must expose both the dense and the sparse vector space named below."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Dense vector space name",
        description = "Name of the dense vector space to use for the dense prefetch branch, as configured on the collection."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> denseVectorName;

    @Schema(
        title = "Dense query vector",
        description = "Dense query embedding, as a list of finite numbers. Its dimension must match the dense vector space of the collection."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> denseVector;

    @Schema(
        title = "Sparse vector space name",
        description = "Name of the sparse vector space to use for the sparse prefetch branch, as configured on the collection."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sparseVectorName;

    @Schema(
        title = "Sparse query indices",
        description = "Indices of the non-zero dimensions of the sparse query vector, as a list of distinct non-negative whole numbers. Must have the same number of elements as 'sparseValues'."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> sparseIndices;

    @Schema(
        title = "Sparse query values",
        description = "Weights of the non-zero dimensions of the sparse query vector, as a list of finite numbers positionally paired with 'sparseIndices'."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> sparseValues;

    @Schema(
        title = "Candidate limit per branch",
        description = "Maximum number of candidates each prefetch branch returns to the fusion step (default: 50, min: 1, max: 10000). Must be greater than or equal to 'limit'. Overridden per branch by 'denseCandidateLimit' and 'sparseCandidateLimit'."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> candidateLimit = Property.ofValue(DEFAULT_CANDIDATE_LIMIT);

    @Schema(
        title = "Candidate limit of the dense branch",
        description = "Overrides 'candidateLimit' for the dense prefetch branch only (min: 1, max: 10000). Must be greater than or equal to 'limit'."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> denseCandidateLimit;

    @Schema(
        title = "Candidate limit of the sparse branch",
        description = "Overrides 'candidateLimit' for the sparse prefetch branch only (min: 1, max: 10000). Must be greater than or equal to 'limit'."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> sparseCandidateLimit;

    @Schema(
        title = "Limit fused results",
        description = "Maximum number of points returned after fusion (default: 10, min: 1, max: 1000)."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> limit = Property.ofValue(DEFAULT_LIMIT);

    @Schema(
        title = "Filter criteria",
        description = "Filter map applied to both prefetch branches and to the fused result, so a point excluded by the filter cannot enter either candidate list. Accepts the same filter shapes as the 'Query' task."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Include payload",
        description = "Whether to include point payload metadata in the result (default: true)."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> withPayload = Property.ofValue(true);

    @Schema(
        title = "Include vectors",
        description = "Whether to include vector embeddings in the result (default: false)."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> withVectors = Property.ofValue(false);

    @Schema(
        title = "Query timeout",
        description = "Optional server-side and client-side bound for the hybrid query, truncated to whole seconds (min: 1 second). Unset means the Qdrant instance applies its own global timeout."
    )
    @PluginProperty(group = "processing")
    private Property<Duration> queryTimeout;

    @Schema(
        title = "Select fetch behavior",
        description = "Output mode for query results: FETCH returns all rows inline, FETCH_ONE returns the first row, and STORE writes all rows to Kestra storage as an ION file and returns the URI (default: STORE)."
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

        List<Float> rDenseVector = toFiniteFloats(renderList(runContext, this.denseVector, "denseVector"), "denseVector");
        if (rDenseVector.isEmpty()) {
            throw new IllegalArgumentException("'denseVector' must contain at least one dimension");
        }

        List<Integer> rSparseIndices = toSparseIndices(renderList(runContext, this.sparseIndices, "sparseIndices"));
        List<Float> rSparseValues = toFiniteFloats(renderList(runContext, this.sparseValues, "sparseValues"), "sparseValues");
        if (rSparseIndices.isEmpty()) {
            throw new IllegalArgumentException("'sparseIndices' and 'sparseValues' must contain at least one entry");
        }
        if (rSparseIndices.size() != rSparseValues.size()) {
            throw new IllegalArgumentException(
                "'sparseIndices' and 'sparseValues' must have the same number of elements, got: "
                    + rSparseIndices.size() + " indices and " + rSparseValues.size() + " values"
            );
        }

        int rLimit = renderBoundedLimit(runContext, this.limit, "limit", DEFAULT_LIMIT, MAX_LIMIT);
        int rCandidateLimit = renderBoundedLimit(runContext, this.candidateLimit, "candidateLimit", DEFAULT_CANDIDATE_LIMIT, MAX_CANDIDATE_LIMIT);
        int rDenseCandidateLimit = this.denseCandidateLimit == null
            ? rCandidateLimit
            : renderBoundedLimit(runContext, this.denseCandidateLimit, "denseCandidateLimit", rCandidateLimit, MAX_CANDIDATE_LIMIT);
        int rSparseCandidateLimit = this.sparseCandidateLimit == null
            ? rCandidateLimit
            : renderBoundedLimit(runContext, this.sparseCandidateLimit, "sparseCandidateLimit", rCandidateLimit, MAX_CANDIDATE_LIMIT);

        requireCandidateBudget("candidateLimit", rCandidateLimit, rLimit);
        requireCandidateBudget("denseCandidateLimit", rDenseCandidateLimit, rLimit);
        requireCandidateBudget("sparseCandidateLimit", rSparseCandidateLimit, rLimit);

        var rWithPayload = runContext.render(this.withPayload).as(Boolean.class).orElse(true);
        var rWithVectors = runContext.render(this.withVectors).as(Boolean.class).orElse(false);
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);

        Duration rQueryTimeout = runContext.render(this.queryTimeout).as(Duration.class).orElse(null);
        Long rTimeoutSeconds = null;
        if (rQueryTimeout != null) {
            rTimeoutSeconds = rQueryTimeout.toSeconds();
            if (rTimeoutSeconds < 1) {
                throw new IllegalArgumentException("'queryTimeout' must be at least 1 second, got: " + rQueryTimeout);
            }
        }

        Common.Filter rFilter = null;
        if (this.filter != null) {
            Map<String, Object> renderedFilter = runContext.render(this.filter).asMap(String.class, Object.class);
            rFilter = toFilter(renderedFilter);
        }

        Points.PrefetchQuery densePrefetch = prefetch(
            QueryFactory.nearest(rDenseVector),
            rDenseVectorName,
            rDenseCandidateLimit,
            rFilter
        );
        Points.PrefetchQuery sparsePrefetch = prefetch(
            QueryFactory.nearest(rSparseValues, rSparseIndices),
            rSparseVectorName,
            rSparseCandidateLimit,
            rFilter
        );

        Points.QueryPoints.Builder queryBuilder = Points.QueryPoints.newBuilder()
            .setCollectionName(rCollectionName)
            .addPrefetch(densePrefetch)
            .addPrefetch(sparsePrefetch)
            .setQuery(QueryFactory.fusion(Points.Fusion.RRF))
            .setLimit(rLimit)
            .setWithPayload(WithPayloadSelectorFactory.enable(rWithPayload))
            .setWithVectors(WithVectorsSelectorFactory.enable(rWithVectors));

        if (rFilter != null) {
            queryBuilder.setFilter(rFilter);
        }
        if (rTimeoutSeconds != null) {
            queryBuilder.setTimeout(rTimeoutSeconds);
        }

        try (var client = buildClient(runContext)) {
            runContext.logger().info(
                "Hybrid querying collection '{}' with dense branch '{}' (candidates={}), sparse branch '{}' (candidates={}), RRF fusion and limit={}",
                rCollectionName, rDenseVectorName, rDenseCandidateLimit, rSparseVectorName, rSparseCandidateLimit, rLimit
            );

            Points.QueryPoints query = queryBuilder.build();
            List<Points.ScoredPoint> results = rQueryTimeout == null
                ? client.queryAsync(query).get()
                : client.queryAsync(query, rQueryTimeout).get();

            return buildFetchOutput(runContext, rFetchType, results, HybridQuery::mapScoredPoint);
        }
    }

    private static Points.PrefetchQuery prefetch(Points.Query query, String using, int candidateLimit, Common.Filter filter) {
        Points.PrefetchQuery.Builder builder = Points.PrefetchQuery.newBuilder()
            .setQuery(query)
            .setUsing(using)
            .setLimit(candidateLimit);
        if (filter != null) {
            builder.setFilter(filter);
        }
        return builder.build();
    }

    private static Map<String, Object> mapScoredPoint(Points.ScoredPoint scoredPoint) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", fromPointId(scoredPoint.getId()));
        map.put("score", scoredPoint.getScore());
        map.put("version", scoredPoint.getVersion());
        if (scoredPoint.getPayloadCount() > 0) {
            map.put("payload", fromPayloadMap(scoredPoint.getPayloadMap()));
        }
        if (scoredPoint.hasVectors()) {
            Object vectorObj = fromVectorsOutput(scoredPoint.getVectors());
            if (vectorObj != null) {
                map.put("vector", vectorObj);
            }
        }
        return map;
    }

    private static List<Object> renderList(RunContext runContext, Property<List<Object>> property, String name)
        throws IllegalVariableEvaluationException {
        if (property == null) {
            throw new IllegalArgumentException("'" + name + "' is required");
        }
        List<Object> rendered = runContext.render(property).asList(Object.class);
        if (rendered == null) {
            throw new IllegalArgumentException("'" + name + "' is required");
        }
        return rendered;
    }

    private static List<Float> toFiniteFloats(List<Object> values, String name) {
        List<Float> result = new ArrayList<>(values.size());
        for (Object value : values) {
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException(
                    "'" + name + "' must be a list of numbers, got element: " + value
                );
            }
            float asFloat = number.floatValue();
            if (Float.isNaN(asFloat) || Float.isInfinite(asFloat)) {
                throw new IllegalArgumentException(
                    "'" + name + "' must contain finite numbers only, got element: " + number
                );
            }
            result.add(asFloat);
        }
        return result;
    }

    private static List<Integer> toSparseIndices(List<Object> values) {
        List<Integer> result = new ArrayList<>(values.size());
        Set<Integer> seen = new LinkedHashSet<>();
        for (Object value : values) {
            if (!(value instanceof Number number)) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must be a list of numbers, got element: " + value
                );
            }
            double asDouble = number.doubleValue();
            if (Double.isNaN(asDouble) || Double.isInfinite(asDouble)) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must contain finite numbers only, got element: " + number
                );
            }
            if (asDouble != Math.rint(asDouble)) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must contain whole numbers, got element: " + number
                );
            }
            if (asDouble < 0) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must contain non-negative indices, got element: " + number
                );
            }
            if (asDouble > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must contain indices lower than or equal to " + Integer.MAX_VALUE + ", got element: " + number
                );
            }
            int index = (int) asDouble;
            if (!seen.add(index)) {
                throw new IllegalArgumentException(
                    "'sparseIndices' must not contain duplicate indices, got index twice: " + index
                );
            }
            result.add(index);
        }
        return result;
    }

    private static int renderBoundedLimit(RunContext runContext, Property<Integer> property, String name, int defaultValue, int max)
        throws IllegalVariableEvaluationException {
        Integer rendered = property == null ? null : runContext.render(property).as(Integer.class).orElse(null);
        if (rendered == null) {
            return defaultValue;
        }
        validateRange(name, rendered, 1, max);
        return rendered;
    }

    private static void requireCandidateBudget(String name, int candidateLimit, int limit) {
        if (candidateLimit < limit) {
            throw new IllegalArgumentException(
                "'" + name + "' must be greater than or equal to 'limit' so fusion has enough candidates, got: "
                    + candidateLimit + " candidates for a limit of " + limit
            );
        }
    }
}
