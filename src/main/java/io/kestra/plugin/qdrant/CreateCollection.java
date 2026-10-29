package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.grpc.Collections;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a collection in Qdrant",
    description = """
        Creates a new collection with a dense vector of the given dimension and distance metric. By default the dense \
        vector is unnamed. Set `vectorName` to give it a name and `sparseVectors` to add named sparse vector spaces, \
        which is the layout `HybridQuery` needs. Collections with several named dense vectors should be created via \
        the Qdrant API or CLI."""
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
        ),
        @Example(
            title = "Create a collection for hybrid retrieval, with a named dense vector and a named sparse vector",
            full = true,
            code = """
                id: qdrant_create_hybrid_collection
                namespace: company.team

                tasks:
                  - id: create_collection
                    type: io.kestra.plugin.qdrant.CreateCollection
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    vectorName: text_dense
                    vectorSize: 1536
                    distance: COSINE
                    sparseVectors:
                      - name: text_sparse
                        modifier: IDF
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
        title = "Dense vector name",
        description = """
            Name of the dense vector space. Unset creates the unnamed default vector. Set it when the collection also \
            holds sparse vectors that are queried together with the dense one, for example by `HybridQuery`."""
    )
    @PluginProperty(group = "main")
    private Property<String> vectorName;

    @Schema(
        title = "Sparse vector spaces",
        description = """
            Named sparse vector spaces to create next to the dense vector. Each name must be unique and different \
            from `vectorName`. Points get their sparse vectors later through `Upsert`."""
    )
    @PluginProperty(group = "main")
    private List<@Valid SparseVectorSpace> sparseVectors;

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
        validateRange("vectorSize", rVectorSize, 1, Integer.MAX_VALUE);
        var rDistance = runContext.render(this.distance).as(Distance.class).orElse(Distance.COSINE);
        var rOnDiskVectors = runContext.render(this.onDiskVectors).as(Boolean.class).orElse(false);
        var rPayloadMemory = runContext.render(this.payloadMemory).as(PayloadMemory.class).orElse(null);
        var rOnDiskPayload = runContext.render(this.onDiskPayload).as(Boolean.class).orElse(null);

        if (rPayloadMemory == null && Boolean.TRUE.equals(rOnDiskPayload)) {
            rPayloadMemory = PayloadMemory.COLD;
        }

        var rVectorName = runContext.render(this.vectorName).as(String.class).orElse(null);
        if (rVectorName != null && rVectorName.isBlank()) {
            throw new IllegalArgumentException("'vectorName' must not be blank, leave it unset for the unnamed default vector");
        }
        var rSparseVectors = renderSparseVectors(runContext, rVectorName);

        runContext.logger().info(
            "Creating Qdrant collection '{}' with size {} and distance {}, dense vector name '{}' and sparse vectors {}",
            rCollectionName, rVectorSize, rDistance, rVectorName, rSparseVectors.keySet()
        );

        Collections.VectorParams vectorParams = Collections.VectorParams.newBuilder()
            .setSize(rVectorSize)
            .setDistance(rDistance.toGrpc())
            .setOnDisk(rOnDiskVectors)
            .build();

        var vectorsConfig = rVectorName == null
            ? Collections.VectorsConfig.newBuilder().setParams(vectorParams).build()
            : Collections.VectorsConfig.newBuilder()
                .setParamsMap(Collections.VectorParamsMap.newBuilder().putMap(rVectorName, vectorParams).build())
                .build();

        var createBuilder = Collections.CreateCollection.newBuilder()
            .setCollectionName(rCollectionName)
            .setVectorsConfig(vectorsConfig);

        if (!rSparseVectors.isEmpty()) {
            createBuilder.setSparseVectorsConfig(Collections.SparseVectorConfig.newBuilder().putAllMap(rSparseVectors).build());
        }

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

    private Map<String, Collections.SparseVectorParams> renderSparseVectors(RunContext runContext, String rVectorName) throws Exception {
        var result = new LinkedHashMap<String, Collections.SparseVectorParams>();
        if (this.sparseVectors == null) {
            return result;
        }

        var seen = new HashSet<String>();
        for (var space : this.sparseVectors) {
            if (space == null) {
                throw new IllegalArgumentException("'sparseVectors' must not contain an empty entry");
            }
            var rName = runContext.render(space.getName()).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("every entry of 'sparseVectors' needs a 'name'"));
            if (rName.isBlank()) {
                throw new IllegalArgumentException("a sparse vector 'name' must not be blank");
            }
            if (rName.equals(rVectorName)) {
                throw new IllegalArgumentException("sparse vector '" + rName + "' has the same name as the dense vector, names must be unique");
            }
            if (!seen.add(rName)) {
                throw new IllegalArgumentException("sparse vector '" + rName + "' is declared twice, names must be unique");
            }

            var index = Collections.SparseIndexConfig.newBuilder();
            var rMemory = runContext.render(space.getMemory()).as(PayloadMemory.class).orElse(null);
            if (rMemory != null) {
                index.setMemory(rMemory.toGrpc());
            }
            var rThreshold = runContext.render(space.getFullScanThreshold()).as(Integer.class).orElse(null);
            if (rThreshold != null) {
                validateRange("sparseVectors[" + rName + "].fullScanThreshold", rThreshold, 0, Integer.MAX_VALUE);
                index.setFullScanThreshold(rThreshold);
            }

            var rModifier = runContext.render(space.getModifier()).as(SparseModifier.class).orElse(SparseModifier.NONE);
            result.put(
                rName,
                Collections.SparseVectorParams.newBuilder().setIndex(index).setModifier(rModifier.toGrpc()).build()
            );
        }
        return result;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SparseVectorSpace {
        @Schema(
            title = "Sparse vector name",
            description = "Name of the sparse vector space, unique in the collection and different from the dense vector name."
        )
        @NotNull
        private Property<String> name;

        @Schema(
            title = "Sparse index memory placement",
            description = """
                Where the sparse index lives: `COLD` (on disk, mmap), `CACHED` (on disk with an in-memory page cache) \
                or `PINNED` (pinned in RAM). Unset keeps the server default."""
        )
        private Property<PayloadMemory> memory;

        @Schema(
            title = "Full scan threshold",
            description = """
                Number of vectors below which Qdrant prefers a full scan over the sparse index (min: 0). This is a \
                count of vectors, not a size in kilobytes as for the dense HNSW index. Unset keeps the server default."""
        )
        private Property<@Min(0) Integer> fullScanThreshold;

        @Schema(
            title = "Weight modifier",
            description = """
                `NONE` (default) uses the stored weights as they are. `IDF` multiplies them by the inverse document \
                frequency computed by Qdrant, as BM25 does."""
        )
        @Builder.Default
        private Property<SparseModifier> modifier = Property.ofValue(SparseModifier.NONE);
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
