package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.VectorFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.Points;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upsert points into a Qdrant collection",
    description = "Inserts or updates points in a Qdrant collection in configurable batch sizes. Reads from inline lists or Kestra storage URIs."
)
@Plugin(
    examples = {
        @Example(
            title = "Upsert points with vectors and payloads into a Qdrant collection",
            full = true,
            code = """
                id: qdrant_upsert
                namespace: company.team

                tasks:
                  - id: upsert_points
                    type: io.kestra.plugin.qdrant.Upsert
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    batchSize: 100
                    points:
                      - id: 1
                        vector: [0.05, 0.61, 0.76, 0.74]
                        payload:
                          city: Berlin
                          country: Germany
                      - id: 2
                        vector: [0.19, 0.81, 0.75, 0.11]
                        payload:
                          city: Paris
                          country: France
                """
        ),
        @Example(
            title = "Upsert points that carry a named dense vector and a named sparse vector, for hybrid retrieval",
            full = true,
            code = """
                id: qdrant_upsert_hybrid
                namespace: company.team

                tasks:
                  - id: upsert_points
                    type: io.kestra.plugin.qdrant.Upsert
                    host: "{{ secret('QDRANT_HOST') }}"
                    apiKey: "{{ secret('QDRANT_API_KEY') }}"
                    tlsEnabled: true
                    collectionName: docs
                    points:
                      - id: 1
                        vectors:
                          text_dense: [0.05, 0.61, 0.76, 0.74]
                          text_sparse:
                            indices: [12, 104, 982]
                            values: [0.84, 1.23, 0.41]
                        payload:
                          title: Invoice 2026-0042
                """
        )
    }
)
public class Upsert extends QdrantConnection implements RunnableTask<Upsert.Output> {

    @Schema(
        title = "Collection name",
        description = "The name of the collection to upsert points into."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(
        title = "Points to upsert",
        description = """
            Points data source, either an inline list of maps or a Kestra storage URI to an ION file. Each point must \
            include a vector (dense 'vector' list or named 'vectors' map) and optionally an 'id' and 'payload' map. \
            In a named 'vectors' map, a dense vector is a list of numbers and a sparse vector is a map with equally long \
            'indices' and 'values' lists.""",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(group = "main", dynamic = true)
    private Object points;

    @Schema(
        title = "Batch size",
        description = "The number of points to send in each upsert batch request (default: 100, min: 1, max: 1000)."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> batchSize = Property.ofValue(100);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        var rBatchSize = runContext.render(this.batchSize).as(Integer.class).orElse(100);
        validateRange("batchSize", rBatchSize, 1, 1000);

        runContext.logger().info("Upserting points into collection '{}' with batch size {}", rCollectionName, rBatchSize);

        AtomicLong totalUpserted = new AtomicLong();

        try (var client = buildClient(runContext)) {
            Data.from(this.points)
                .read(runContext)
                .map(this::mapToPointStruct)
                .buffer(rBatchSize)
                .concatMap(batch -> Mono.fromFuture(toCompletableFuture(client.upsertAsync(rCollectionName, batch)))
                    .doOnSuccess(res -> {
                        totalUpserted.addAndGet(batch.size());
                        runContext.logger().debug("Upserted batch of {} points", batch.size());
                    })
                    .onErrorMap(e -> new RuntimeException("Failed to upsert points to Qdrant: " + e.getMessage(), e))
                )
                .blockLast();

            runContext.logger().info("Successfully upserted {} points into collection '{}'", totalUpserted.get(), rCollectionName);

            return Output.builder()
                .upsertedCount(totalUpserted.get())
                .build();
        }
    }

    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        Futures.addCallback(
            listenableFuture,
            new FutureCallback<>() {
                @Override
                public void onSuccess(T result) {
                    completableFuture.complete(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    completableFuture.completeExceptionally(t);
                }
            },
            MoreExecutors.directExecutor()
        );
        return completableFuture;
    }

    @SuppressWarnings("unchecked")
    private Points.PointStruct mapToPointStruct(Map<String, Object> map) {
        var builder = Points.PointStruct.newBuilder();

        Object id = map.get("id");
        if (id != null) {
            builder.setId(toPointId(id));
        } else {
            builder.setId(PointIdFactory.id(UUID.randomUUID()));
        }

        Object vector = map.get("vector");
        if (vector == null) {
            vector = map.get("vectors");
        }
        if (vector instanceof List<?> list) {
            try {
                List<Float> floats = list.stream().map(n -> ((Number) n).floatValue()).toList();
                builder.setVectors(VectorsFactory.vectors(floats));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Point '" + map.get("id") + "': vector list must contain only numbers", e);
            }
        } else if (vector instanceof Map<?, ?> vMap) {
            Map<String, Points.Vector> named = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : vMap.entrySet()) {
                String name = String.valueOf(entry.getKey());
                if (entry.getValue() instanceof Map<?, ?> sparse) {
                    named.put(name, toSparseVector(map.get("id"), name, sparse));
                } else if (entry.getValue() instanceof List<?> vList) {
                    try {
                        List<Float> floats = vList.stream().map(n -> ((Number) n).floatValue()).toList();
                        named.put(name, Points.Vector.newBuilder().addAllData(floats).build());
                    } catch (ClassCastException e) {
                        throw new IllegalArgumentException("Point '" + map.get("id") + "': named vector '" + name + "' must contain only numbers", e);
                    }
                } else {
                    throw new IllegalArgumentException("Point '" + map.get("id") + "': named vector '" + name + "' must be a list of numbers, or a map with 'indices' and 'values' for a sparse vector");
                }
            }
            builder.setVectors(VectorsFactory.namedVectors(named));
        } else {
            throw new IllegalArgumentException("Point '" + map.get("id") + "': must contain a 'vector' or 'vectors' field as a list or map of numbers");
        }

        Object payload = map.get("payload");
        if (payload instanceof Map<?, ?> pMap) {
            builder.putAllPayload(toPayloadMap((Map<String, Object>) pMap));
        }

        return builder.build();
    }

    private static Points.Vector toSparseVector(Object pointId, String name, Map<?, ?> sparse) {
        var where = "Point '" + pointId + "': sparse vector '" + name + "'";
        for (var key : sparse.keySet()) {
            if (!"indices".equals(key) && !"values".equals(key)) {
                throw new IllegalArgumentException(where + " has an unknown field '" + key + "', only 'indices' and 'values' are allowed");
            }
        }
        if (!(sparse.get("indices") instanceof List<?> rawIndices) || !(sparse.get("values") instanceof List<?> rawValues)) {
            throw new IllegalArgumentException(where + " needs both an 'indices' list and a 'values' list");
        }

        try {
            var indices = toSparseIndices(new ArrayList<Object>(rawIndices), "indices");
            var values = toFiniteFloats(new ArrayList<Object>(rawValues), "values");
            if (indices.size() != values.size()) {
                throw new IllegalArgumentException(
                    "'indices' and 'values' must have the same number of elements, got: "
                        + indices.size() + " indices and " + values.size() + " values"
                );
            }
            if (indices.isEmpty()) {
                throw new IllegalArgumentException("'indices' and 'values' must contain at least one entry");
            }
            return VectorFactory.vector(values, indices);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + ": " + e.getMessage(), e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of upserted points",
            description = "The total count of points successfully upserted into the collection."
        )
        private final long upsertedCount;
    }
}
