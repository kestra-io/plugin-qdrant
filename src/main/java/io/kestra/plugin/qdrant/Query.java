package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.WithPayloadSelectorFactory;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query points in a Qdrant collection",
    description = "Searches for nearest neighbors in a Qdrant collection using a vector or the vector of an existing point ID. Supports filters and score threshold."
)
@Plugin(
    examples = {
        @Example(
            title = "Search for nearest vectors and return inline rows",
            full = true,
            code = """
                id: qdrant_query
                namespace: company.team

                tasks:
                  - id: search_points
                    type: io.kestra.plugin.qdrant.Query
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vector: [0.05, 0.61, 0.76, 0.74]
                    topK: 5
                    withPayload: true
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Search using an existing point ID and store results to Kestra storage",
            full = true,
            code = """
                id: qdrant_query_by_id
                namespace: company.team

                tasks:
                  - id: search_by_id
                    type: io.kestra.plugin.qdrant.Query
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vectorId: 1
                    topK: 10
                    fetchType: STORE
                """
        )
    }
)
public class Query extends QdrantConnection implements RunnableTask<FetchOutput> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to search in."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Query vector",
        description = "Dense vector to search for nearest neighbors. Mutually exclusive with 'vectorId'."
    )
    @PluginProperty(group = "main")
    private Property<List<Object>> vector;

    @Schema(
        title = "Point ID to use as query vector",
        description = "Existing point ID whose vector should be used for searching. Mutually exclusive with 'vector'."
    )
    @PluginProperty(group = "main")
    private Property<Object> vectorId;

    @Schema(
        title = "Vector name",
        description = "Name of the vector space to target when querying a collection configured with named vectors."
    )
    @PluginProperty(group = "main")
    private Property<String> vectorName;

    @Schema(
        title = "Limit results",
        description = "Maximum number of nearest points to return (default: 10, min: 1, max: 1000). Takes precedence if 'topK' is also set."
    )
    @Min(1)
    @Max(1000)
    @PluginProperty(group = "processing")
    private Property<Integer> limit;

    @Schema(
        title = "Top K results",
        description = "Maximum number of nearest points to return (default: 10, min: 1, max: 1000). Alias for 'limit'; overridden if 'limit' is also specified."
    )
    @Min(1)
    @Max(1000)
    @PluginProperty(group = "processing")
    private Property<Integer> topK;

    @Schema(
        title = "Filter criteria",
        description = "Filter map to restrict the search space before ranking."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Score threshold",
        description = "Optional minimal similarity score threshold for returning points."
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
        title = "Select fetch behavior",
        description = "Output mode for query results: FETCH returns all rows inline, FETCH_ONE returns the first row, and STORE writes all rows to Kestra storage as an ION file and returns the URI (default: STORE)."
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        boolean hasVector = this.vector != null;
        boolean hasVectorId = this.vectorId != null;

        if (hasVector == hasVectorId) {
            throw new IllegalArgumentException(
                "Exactly one of `vector` or `vectorId` must be specified. Please provide either a query vector or a point ID, not both or neither."
            );
        }

        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        int rLimit = resolveLimit(runContext);

        var rVectorName = this.vectorName != null ? runContext.render(this.vectorName).as(String.class).orElse(null) : null;
        var rWithPayload = runContext.render(this.withPayload).as(Boolean.class).orElse(true);
        var rWithVectors = runContext.render(this.withVectors).as(Boolean.class).orElse(false);
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);
        Float rScoreThreshold = runContext.render(this.scoreThreshold).as(Float.class).orElse(null);

        try (var client = buildClient(runContext)) {
            List<Float> queryFloats;
            if (hasVector) {
                List<Object> rawList = runContext.render(this.vector).asList(Object.class);
                queryFloats = rawList.stream().map(n -> ((Number) n).floatValue()).toList();
            } else {
                Object rawId = runContext.render(this.vectorId).as(Object.class).orElseThrow(() -> new IllegalArgumentException("'vectorId' is required"));
                Common.PointId pid = toPointId(rawId);
                var retrieved = client.retrieveAsync(
                    rCollectionName,
                    List.of(pid),
                    WithPayloadSelectorFactory.enable(false),
                    WithVectorsSelectorFactory.enable(true),
                    null
                ).get();
                if (retrieved.isEmpty() || !retrieved.getFirst().hasVectors()) {
                    throw new IllegalArgumentException("Point with ID '" + rawId + "' not found or has no vector in collection '" + rCollectionName + "'");
                }
                var vOut = retrieved.getFirst().getVectors();
                if (vOut.hasVector()) {
                    queryFloats = extractVectorData(vOut.getVector());
                } else if (vOut.hasVectors()) {
                    Map<String, Points.VectorOutput> namedMap = vOut.getVectors().getVectorsMap();
                    if (rVectorName != null) {
                        Points.VectorOutput vo = namedMap.get(rVectorName);
                        if (vo == null) {
                            throw new IllegalArgumentException("Point with ID '" + rawId + "' does not contain a named vector '" + rVectorName + "'. Available vectors: " + namedMap.keySet());
                        }
                        queryFloats = extractVectorData(vo);
                    } else if (namedMap.size() == 1) {
                        var entry = namedMap.entrySet().iterator().next();
                        rVectorName = entry.getKey();
                        queryFloats = extractVectorData(entry.getValue());
                    } else {
                        throw new IllegalArgumentException("Point with ID '" + rawId + "' contains multiple named vectors " + namedMap.keySet() + ". Please specify 'vectorName' to select which vector to use.");
                    }
                } else {
                    throw new IllegalArgumentException("Point with ID '" + rawId + "' does not contain any vectors in collection '" + rCollectionName + "'");
                }
                if (queryFloats.isEmpty()) {
                    throw new IllegalArgumentException("Point with ID '" + rawId + "' has empty vector in collection '" + rCollectionName + "'");
                }
            }

            Points.SearchPoints.Builder searchBuilder = Points.SearchPoints.newBuilder()
                .setCollectionName(rCollectionName)
                .addAllVector(queryFloats)
                .setLimit(rLimit)
                .setWithPayload(WithPayloadSelectorFactory.enable(rWithPayload))
                .setWithVectors(WithVectorsSelectorFactory.enable(rWithVectors));

            if (rVectorName != null) {
                searchBuilder.setVectorName(rVectorName);
            }

            if (this.filter != null) {
                Map<String, Object> renderedFilter = runContext.render(this.filter).asMap(String.class, Object.class);
                Common.Filter filterProto = toFilter(renderedFilter);
                if (filterProto != null) {
                    searchBuilder.setFilter(filterProto);
                }
            }

            if (rScoreThreshold != null) {
                searchBuilder.setScoreThreshold(rScoreThreshold);
            }

            runContext.logger().info("Searching collection '{}' with limit={}", rCollectionName, rLimit);
            List<Points.ScoredPoint> results = client.searchAsync(searchBuilder.build()).get();

            return buildFetchOutput(runContext, rFetchType, results, this::mapScoredPoint);
        }
    }

    private Map<String, Object> mapScoredPoint(Points.ScoredPoint sp) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", fromPointId(sp.getId()));
        map.put("score", sp.getScore());
        map.put("version", sp.getVersion());
        if (sp.getPayloadCount() > 0) {
            map.put("payload", fromPayloadMap(sp.getPayloadMap()));
        }
        if (sp.hasVectors()) {
            Object vectorObj = fromVectorsOutput(sp.getVectors());
            if (vectorObj != null) {
                map.put("vector", vectorObj);
            }
        }
        return map;
    }

    int resolveLimit(RunContext runContext) throws io.kestra.core.exceptions.IllegalVariableEvaluationException {
        Integer rLimit = null;
        if (this.limit != null) {
            rLimit = runContext.render(this.limit).as(Integer.class).orElse(null);
        }
        if (rLimit == null && this.topK != null) {
            rLimit = runContext.render(this.topK).as(Integer.class).orElse(null);
        }
        return rLimit != null ? rLimit : 10;
    }
}
