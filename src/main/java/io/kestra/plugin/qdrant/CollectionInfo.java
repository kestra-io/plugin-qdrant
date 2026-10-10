package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get collection details and statistics from Qdrant",
    description = "Retrieves information about a collection, including point counts, status, vector dimensions, and distance metric."
)
@Plugin(
    examples = {
        @Example(
            title = "Retrieve information about a Qdrant collection",
            full = true,
            code = """
                id: qdrant_collection_info
                namespace: company.team

                tasks:
                  - id: get_info
                    type: io.kestra.plugin.qdrant.CollectionInfo
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                """
        )
    }
)
public class CollectionInfo extends QdrantConnection implements RunnableTask<CollectionInfo.Output> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to inspect."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        runContext.logger().info("Getting collection info for Qdrant collection '{}'", rCollectionName);

        try (var client = buildClient(runContext)) {
            var info = client.getCollectionInfoAsync(rCollectionName).get();

            Long pointsCount = info.hasPointsCount() ? info.getPointsCount() : null;
            Long vectorsCount = info.hasIndexedVectorsCount() ? info.getIndexedVectorsCount() : null;
            String status = info.getStatus() != null ? info.getStatus().name() : null;

            Long vectorSize = null;
            String distance = null;
            if (info.hasConfig() && info.getConfig().hasParams() && info.getConfig().getParams().hasVectorsConfig()) {
                var vc = info.getConfig().getParams().getVectorsConfig();
                if (vc.hasParams()) {
                    vectorSize = vc.getParams().getSize();
                    distance = vc.getParams().getDistance().name();
                } else if (vc.hasParamsMap() && !vc.getParamsMap().getMapMap().isEmpty()) {
                    var first = vc.getParamsMap().getMapMap().values().iterator().next();
                    vectorSize = first.getSize();
                    distance = first.getDistance().name();
                }
            }

            return Output.builder()
                .collectionName(rCollectionName)
                .status(status)
                .pointsCount(pointsCount)
                .indexedVectorsCount(vectorsCount)
                .vectorSize(vectorSize)
                .distance(distance)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Collection name",
            description = "The name of the collection inspected."
        )
        private final String collectionName;

        @Schema(
            title = "Collection status",
            description = "Current operational status of the collection: Green (healthy), Yellow (optimizing/degraded), Red (error/unavailable), or Grey (optimization pending)."
        )
        private final String status;

        @Schema(
            title = "Points count",
            description = "Approximate total number of points stored in the collection."
        )
        private final Long pointsCount;

        @Schema(
            title = "Indexed vectors count",
            description = "Approximate number of vectors indexed for approximate nearest neighbor search."
        )
        private final Long indexedVectorsCount;

        @Schema(
            title = "Vector dimension size",
            description = "Dimension size of vectors configured for this collection."
        )
        private final Long vectorSize;

        @Schema(
            title = "Distance metric",
            description = "Distance metric configured for this collection."
        )
        private final String distance;
    }
}
