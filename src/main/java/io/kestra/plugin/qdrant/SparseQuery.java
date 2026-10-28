package io.kestra.plugin.qdrant;

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
    title = "Query a Qdrant collection with a sparse vector",
    description = """
        Runs one sparse-only query against a named sparse vector space and returns the nearest points with their \
        sparse similarity score.

        This is the sparse counterpart of `Query` and the third leg of a retrieval comparison next to `Query` (dense) \
        and `HybridQuery` (fused): running the same labelled queries through all three shows whether fusion improves \
        your workload. The collection must already expose the sparse vector space, which `CreateCollection` can create \
        and `Upsert` can fill."""
)
@Plugin(
    examples = {
        @Example(
            title = "Search a sparse vector space for exact identifiers or lexical matches",
            full = true,
            code = """
                id: qdrant_sparse_query
                namespace: company.team

                tasks:
                  - id: sparse_search
                    type: io.kestra.plugin.qdrant.SparseQuery
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vectorName: text_sparse
                    indices: [12, 104, 982]
                    values: [0.84, 1.23, 0.41]
                    limit: 10
                    filter:
                      tenant_id: acme
                    fetchType: FETCH
                """
        )
    }
)
public class SparseQuery extends QdrantConnection implements RunnableTask<FetchOutput> {

    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 1000;
    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(30);

    @Schema(
        title = "Collection name",
        description = "The name of the collection to search in. It must expose the sparse vector space named below."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Sparse vector space name",
        description = "Name of the sparse vector space to search, as configured on the collection."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> vectorName;

    @Schema(
        title = "Sparse query indices",
        description = """
            Indices of the non-zero dimensions of the sparse query vector, as a list of distinct non-negative whole \
            numbers. Must have the same number of elements as 'values'."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> indices;

    @Schema(
        title = "Sparse query values",
        description = "Weights of the non-zero dimensions of the sparse query vector, as a list of finite numbers positionally paired with 'indices'."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> values;

    @Schema(
        title = "Limit results",
        description = "Maximum number of points to return (default: 10, min: 1, max: 1000)."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> limit = Property.ofValue(DEFAULT_LIMIT);

    @Schema(
        title = "Filter criteria",
        description = "Filter map to restrict the search space before ranking. Accepts the same filter shapes as the 'Query' task."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Score threshold",
        description = "Optional minimal sparse similarity score for returning points."
    )
    @PluginProperty(group = "processing")
    private Property<Float> scoreThreshold;

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
        description = """
            Server-side and client-side bound for the query, truncated to whole seconds (default: PT30S, min: 1 \
            second). The same value is sent as the Qdrant request timeout and used as the gRPC call deadline."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Duration> queryTimeout = Property.ofValue(DEFAULT_QUERY_TIMEOUT);

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
        var rVectorName = runContext.render(this.vectorName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'vectorName' is required"));

        var rIndices = toSparseIndices(renderList(runContext, this.indices, "indices"), "indices");
        var rValues = toFiniteFloats(renderList(runContext, this.values, "values"), "values");
        if (rIndices.isEmpty()) {
            throw new IllegalArgumentException("'indices' and 'values' must contain at least one entry");
        }
        if (rIndices.size() != rValues.size()) {
            throw new IllegalArgumentException(
                "'indices' and 'values' must have the same number of elements, got: "
                    + rIndices.size() + " indices and " + rValues.size() + " values"
            );
        }

        var rLimit = runContext.render(this.limit).as(Integer.class).orElse(DEFAULT_LIMIT);
        validateRange("limit", rLimit, 1, MAX_LIMIT);

        var rWithPayload = runContext.render(this.withPayload).as(Boolean.class).orElse(true);
        var rWithVectors = runContext.render(this.withVectors).as(Boolean.class).orElse(false);
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);
        var rScoreThreshold = this.scoreThreshold == null ? null : runContext.render(this.scoreThreshold).as(Float.class).orElse(null);

        var rQueryTimeout = runContext.render(this.queryTimeout).as(Duration.class).orElse(DEFAULT_QUERY_TIMEOUT);
        var rTimeoutSeconds = rQueryTimeout.toSeconds();
        if (rTimeoutSeconds < 1) {
            throw new IllegalArgumentException("'queryTimeout' must be at least 1 second, got: " + rQueryTimeout);
        }

        var queryBuilder = Points.QueryPoints.newBuilder()
            .setCollectionName(rCollectionName)
            .setQuery(QueryFactory.nearest(rValues, rIndices))
            .setUsing(rVectorName)
            .setLimit(rLimit)
            .setTimeout(rTimeoutSeconds)
            .setWithPayload(WithPayloadSelectorFactory.enable(rWithPayload))
            .setWithVectors(WithVectorsSelectorFactory.enable(rWithVectors));

        if (this.filter != null) {
            var rFilterMap = runContext.render(this.filter).asMap(String.class, Object.class);
            var rFilter = toFilter(rFilterMap);
            if (rFilter != null) {
                queryBuilder.setFilter(rFilter);
            }
        }
        if (rScoreThreshold != null) {
            queryBuilder.setScoreThreshold(rScoreThreshold);
        }

        try (var client = buildClient(runContext)) {
            runContext.logger().info(
                "Sparse querying collection '{}' on vector space '{}' with {} non-zero dimensions, limit={} and timeout={}",
                rCollectionName, rVectorName, rIndices.size(), rLimit, rQueryTimeout
            );

            var results = client.queryAsync(queryBuilder.build(), rQueryTimeout).get();

            return buildFetchOutput(runContext, rFetchType, results, QdrantConnection::mapScoredPoint);
        }
    }

}
