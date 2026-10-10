package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
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
    title = "Delete a collection in Qdrant",
    description = "Deletes an existing vector collection and all points contained within it."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a Qdrant collection",
            full = true,
            code = """
                id: qdrant_delete_collection
                namespace: company.team

                tasks:
                  - id: delete_collection
                    type: io.kestra.plugin.qdrant.DeleteCollection
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                """
        )
    }
)
public class DeleteCollection extends QdrantConnection implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to delete."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        runContext.logger().info("Deleting Qdrant collection '{}'", rCollectionName);

        try (var client = buildClient(runContext)) {
            var response = client.deleteCollectionAsync(rCollectionName).get();
            runContext.logger().info("Collection '{}' deleted successfully: result={}", rCollectionName, response.getResult());
            return null;
        }
    }
}
