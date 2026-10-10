package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.grpc.Collections;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
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
    title = "Create a collection in Qdrant",
    description = "Creates a new single-vector collection with specified vector dimension and distance metric. Collections with multiple named vectors should be created via the Qdrant API or CLI."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a Qdrant collection with 1536 dimensions and Cosine distance",
            full = true,
            code = """
                id: qdrant_create_collection
                namespace: company.team

                tasks:
                  - id: create_collection
                    type: io.kestra.plugin.qdrant.CreateCollection
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vectorSize: 1536
                    distance: COSINE
                """
        )
    }
)
public class CreateCollection extends QdrantConnection implements RunnableTask<CreateCollection.Output> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to create."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Vector dimension size",
        description = "The dimension size of vectors to be stored in the collection."
    )
    @NotNull
    @Min(1)
    @PluginProperty(group = "main")
    private Property<Integer> vectorSize;

    @Schema(
        title = "Distance metric",
        description = "The distance metric to use for vector comparisons (default: COSINE)."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Distance> distance = Property.ofValue(Distance.COSINE);

    @Schema(
        title = "Payload memory storage policy",
        description = "Storage memory tier for metadata payload: COLD (on disk/mmap), CACHED (on disk with memory page cache), or PINNED (pinned in RAM). Default: null (server default)."
    )
    @PluginProperty(group = "advanced")
    private Property<PayloadMemory> payloadMemory;

    @Schema(
        title = "Store vectors on disk",
        description = "Whether to store vector embeddings on disk (mmap) rather than keeping them entirely in RAM (default: false)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> onDiskVectors = Property.ofValue(false);

    @Schema(
        title = "Store payload on disk (legacy alias)",
        description = "Legacy boolean alias for payload memory policy. When true, sets payload memory to COLD. Deprecated in Qdrant 1.19: prefer using 'payloadMemory: COLD'."
    )
    @Deprecated
    @PluginProperty(group = "advanced")
    private Property<Boolean> onDiskPayload;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        var rVectorSize = runContext.render(this.vectorSize).as(Integer.class).orElseThrow(() -> new IllegalArgumentException("'vectorSize' is required"));
        var rDistance = runContext.render(this.distance).as(Distance.class).orElse(Distance.COSINE);
        var rOnDiskVectors = runContext.render(this.onDiskVectors).as(Boolean.class).orElse(false);
        var rPayloadMemory = runContext.render(this.payloadMemory).as(PayloadMemory.class).orElse(null);
        var rOnDiskPayload = runContext.render(this.onDiskPayload).as(Boolean.class).orElse(null);

        if (rPayloadMemory == null && Boolean.TRUE.equals(rOnDiskPayload)) {
            rPayloadMemory = PayloadMemory.COLD;
        }

        runContext.logger().info("Creating Qdrant collection '{}' with size {} and distance {}", rCollectionName, rVectorSize, rDistance);

        Collections.VectorParams vectorParams = Collections.VectorParams.newBuilder()
            .setSize(rVectorSize)
            .setDistance(rDistance.toGrpc())
            .setOnDisk(rOnDiskVectors)
            .build();

        Collections.CreateCollection.Builder createBuilder = Collections.CreateCollection.newBuilder()
            .setCollectionName(rCollectionName)
            .setVectorsConfig(Collections.VectorsConfig.newBuilder()
                .setParams(vectorParams)
                .build());

        if (rPayloadMemory != null) {
            createBuilder.setPayload(Collections.PayloadStorageParams.newBuilder()
                .setMemory(rPayloadMemory.toGrpc())
                .build());
        }

        try (var client = buildClient(runContext)) {
            var response = client.createCollectionAsync(createBuilder.build()).get();
            runContext.logger().info("Collection '{}' created successfully: result={}", rCollectionName, response.getResult());

            return Output.builder()
                .collectionName(rCollectionName)
                .success(response.getResult())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Collection name",
            description = "The name of the collection that was created."
        )
        private final String collectionName;

        @Schema(
            title = "Success status",
            description = "Whether the collection was created successfully."
        )
        private final Boolean success;
    }
}
