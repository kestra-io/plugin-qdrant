package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete points from a Qdrant collection",
    description = "Deletes points from a collection by their IDs or by matching a filter criteria. Exactly one of 'ids' or 'filter' must be specified."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete specific points by ID",
            full = true,
            code = """
                id: qdrant_delete_by_id
                namespace: company.team

                tasks:
                  - id: delete_points
                    type: io.kestra.plugin.qdrant.Delete
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    ids: [1, 2, "38f72df0-1845-42a9-8350-f8da2b369ec2"]
                """
        ),
        @Example(
            title = "Delete points matching filter criteria",
            full = true,
            code = """
                id: qdrant_delete_by_filter
                namespace: company.team

                tasks:
                  - id: delete_points
                    type: io.kestra.plugin.qdrant.Delete
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    filter:
                      city: Berlin
                """
        )
    }
)
public class Delete extends QdrantConnection implements RunnableTask<Delete.Output> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to delete points from."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Point IDs to delete",
        description = "List of point IDs to delete (numbers or UUID strings). Mutually exclusive with 'filter'."
    )
    @PluginProperty(group = "main")
    private Property<List<Object>> ids;

    @Schema(
        title = "Filter criteria",
        description = "Filter map to match points to delete. Mutually exclusive with 'ids'."
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> filter;

    @Override
    public Output run(RunContext runContext) throws Exception {
        boolean hasIds = this.ids != null;
        boolean hasFilter = this.filter != null;

        if (hasIds == hasFilter) {
            throw new IllegalArgumentException(
                "Exactly one of `ids` or `filter` must be specified. Please provide either a list of point IDs or a filter map, not both or neither."
            );
        }

        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));

        try (var client = buildClient(runContext)) {
            if (hasIds) {
                List<Object> renderedIds = runContext.render(this.ids).asList(Object.class);
                if (renderedIds.isEmpty()) {
                    return Output.builder()
                        .deletedCount(0L)
                        .success(true)
                        .build();
                }
                List<Common.PointId> pointIds = renderedIds.stream()
                    .map(QdrantConnection::toPointId)
                    .toList();

                runContext.logger().info("Deleting {} points by ID from collection '{}'", pointIds.size(), rCollectionName);
                var result = client.deleteAsync(rCollectionName, pointIds).get();

                boolean success = result.getStatus() == Points.UpdateStatus.Completed
                    || result.getStatus() == Points.UpdateStatus.Acknowledged;

                return Output.builder()
                    .deletedCount((long) pointIds.size())
                    .success(success)
                    .build();
            } else {
                Map<String, Object> renderedFilter = runContext.render(this.filter).asMap(String.class, Object.class);
                runContext.logger().info("Deleting points matching filter from collection '{}'", rCollectionName);
                Common.Filter filterProto = toFilter(renderedFilter);
                var result = client.deleteAsync(rCollectionName, filterProto).get();

                boolean success = result.getStatus() == Points.UpdateStatus.Completed
                    || result.getStatus() == Points.UpdateStatus.Acknowledged;

                return Output.builder()
                    .success(success)
                    .build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of deleted points",
            description = "The count of points targeted for deletion (available when deleting by IDs)."
        )
        private final Long deletedCount;

        @Schema(
            title = "Success status",
            description = "Whether the delete operation was acknowledged or completed successfully."
        )
        private final Boolean success;
    }
}
