package io.kestra.plugin.qdrant;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.VectorsFactory;
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import reactor.core.publisher.Mono;

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
        description = "Points data source, either an inline list of maps or a Kestra storage URI to an ION file. Each point must include a vector (dense 'vector' list or named 'vectors' map) and optionally an 'id' and 'payload' map."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Data points;

    @Schema(
        title = "Batch size",
        description = "The number of points to send in each upsert batch request (default: 100, min: 1, max: 1000)."
    )
    @Builder.Default
    @Min(1)
    @Max(1000)
    @PluginProperty(group = "processing")
    private Property<Integer> batchSize = Property.ofValue(100);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(this.collectionName).as(String.class).orElseThrow(() -> new IllegalArgumentException("'collectionName' is required"));
        var rBatchSize = runContext.render(this.batchSize).as(Integer.class).orElse(100);

        runContext.logger().info("Upserting points into collection '{}' with batch size {}", rCollectionName, rBatchSize);

        AtomicLong totalUpserted = new AtomicLong();

        try (var client = buildClient(runContext)) {
            this.points.read(runContext)
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
                if (entry.getValue() instanceof List<?> vList) {
                    try {
                        List<Float> floats = vList.stream().map(n -> ((Number) n).floatValue()).toList();
                        named.put(name, Points.Vector.newBuilder().addAllData(floats).build());
                    } catch (ClassCastException e) {
                        throw new IllegalArgumentException("Point '" + map.get("id") + "': named vector '" + name + "' must contain only numbers", e);
                    }
                } else {
                    throw new IllegalArgumentException("Point '" + map.get("id") + "': named vector '" + name + "' must be a list of numbers");
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
