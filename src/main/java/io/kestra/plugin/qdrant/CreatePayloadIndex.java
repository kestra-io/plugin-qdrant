package io.kestra.plugin.qdrant;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
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
    title = "Create a Qdrant payload index",
    description = """
        Indexes a payload field so filtered queries can use it, including collections with strict-mode filtering.
        Waits for the index operation to complete by default. Set wait to false to submit it asynchronously,
        then inspect CollectionInfo.payloadSchema before relying on the index.
        """
)
@Plugin(examples = {
    @Example(full = true, title = "Index a tenant field for filtered queries", code = """
        id: qdrant_create_payload_index
        namespace: company.team
        tasks:
          - id: create_index
            type: io.kestra.plugin.qdrant.CreatePayloadIndex
            host: "{{ secret('QDRANT_HOST') }}"
            apiKey: "{{ secret('QDRANT_API_KEY') }}"
            tlsEnabled: true
            collectionName: docs
            fieldName: tenant_id
            fieldSchema: KEYWORD
            wait: true
        """)
})
public class CreatePayloadIndex extends QdrantConnection implements RunnableTask<CreatePayloadIndex.Output> {
    @Schema(title = "Collection name", description = "The existing collection containing the payload field.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> collectionName;

    @Schema(title = "Payload field name", description = "The payload field to index, including dotted paths for nested fields.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fieldName;

    @Schema(title = "Payload field schema", description = "The data type of the indexed payload field.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<PayloadSchema> fieldSchema;

    @Schema(title = "Wait for completion", description = "Defaults to true. False returns after the operation is acknowledged, before the index is necessarily ready.")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(true);

    @Schema(title = "Request timeout", description = "Maximum duration of the gRPC request. Must be positive; defaults to PT30S.")
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Duration> requestTimeout = Property.ofValue(Duration.ofSeconds(30));

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rCollectionName = runContext.render(collectionName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("collectionName is required. Set the existing Qdrant collection name."));
        var rFieldName = runContext.render(fieldName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("fieldName is required. Set the payload field to index."));
        var rFieldSchema = runContext.render(fieldSchema).as(PayloadSchema.class)
            .orElseThrow(() -> new IllegalArgumentException("fieldSchema is required. Select the payload field's data type."));
        var rWait = runContext.render(wait).as(Boolean.class).orElse(true);
        var rRequestTimeout = runContext.render(requestTimeout).as(Duration.class).orElse(Duration.ofSeconds(30));
        if (rCollectionName.isBlank() || rFieldName.isBlank()) {
            throw new IllegalArgumentException("collectionName and fieldName must not be blank. Set the collection and payload field names.");
        }
        if (rRequestTimeout.isNegative() || rRequestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive. Set a duration such as PT30S.");
        }

        try (var client = buildClient(runContext)) {
            var result = client.createPayloadIndexAsync(
                rCollectionName, rFieldName, rFieldSchema.toGrpc(), null, rWait, null, rRequestTimeout
            ).get();
            return Output.builder()
                .operationId(result.hasOperationId() ? result.getOperationId() : null)
                .status(result.getStatus().name())
                .build();
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Could not create payload index '%s' in collection '%s'. Check the collection name, field schema and write permissions.".formatted(rFieldName, rCollectionName),
                e.getCause()
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Operation ID", description = "Sequential operation identifier, when reported by Qdrant.")
        private final Long operationId;

        @Schema(title = "Operation status", description = "Acknowledged means queued; Completed means applied. Inspect this status when wait is false.")
        private final String status;
    }
}
