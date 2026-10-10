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
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieve points by ID from a Qdrant collection",
    description = "Fetches points from a collection by a list of point IDs. Supports FETCH, FETCH_ONE, and STORE output modes."
)
@Plugin(
    examples = {
        @Example(
            title = "Get points by ID and return inline rows",
            full = true,
            code = """
                id: qdrant_get
                namespace: company.team

                tasks:
                  - id: get_points
                    type: io.kestra.plugin.qdrant.Get
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    ids: [1, 2]
                    withPayload: true
                    fetchType: FETCH
                """
        )
    }
)
public class Get extends QdrantConnection implements RunnableTask<FetchOutput> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to retrieve points from."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Point IDs to retrieve",
        description = "List of point IDs to retrieve (numbers or UUID strings)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Object>> ids;

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
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        List<Object> renderedIds = runContext.render(this.ids).asList(Object.class);
        var rWithPayload = runContext.render(this.withPayload).as(Boolean.class).orElse(true);
        var rWithVectors = runContext.render(this.withVectors).as(Boolean.class).orElse(false);
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);

        if (renderedIds.isEmpty()) {
            return buildFetchOutput(runContext, rFetchType, Collections.emptyList());
        }

        List<Common.PointId> pointIds = renderedIds.stream()
            .map(QdrantConnection::toPointId)
            .toList();

        runContext.logger().info("Retrieving {} points from collection '{}'", pointIds.size(), rCollectionName);

        try (var client = buildClient(runContext)) {
            List<Points.RetrievedPoint> retrieved = client.retrieveAsync(
                rCollectionName,
                pointIds,
                WithPayloadSelectorFactory.enable(rWithPayload),
                WithVectorsSelectorFactory.enable(rWithVectors),
                null
            ).get();

            return buildFetchOutput(runContext, rFetchType, retrieved, this::mapRetrievedPoint);
        }
    }

    private Map<String, Object> mapRetrievedPoint(Points.RetrievedPoint point) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", fromPointId(point.getId()));
        if (point.getPayloadCount() > 0) {
            map.put("payload", fromPayloadMap(point.getPayloadMap()));
        }
        if (point.hasVectors()) {
            Object vectorObj = fromVectorsOutput(point.getVectors());
            if (vectorObj != null) {
                map.put("vector", vectorObj);
            }
        }
        return map;
    }
}
