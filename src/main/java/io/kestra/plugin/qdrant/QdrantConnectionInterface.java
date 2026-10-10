package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface QdrantConnectionInterface {
    @Schema(
        title = "The host of the Qdrant instance",
        description = "The hostname or IP address of the Qdrant gRPC endpoint."
    )
    Property<String> getHost();

    @Schema(
        title = "The port of the Qdrant instance",
        description = "The gRPC port of the Qdrant instance (default 6334)."
    )
    Property<Integer> getPort();

    @Schema(
        title = "API key",
        description = "The API key for authentication with Qdrant."
    )
    Property<String> getApiKey();

    @Schema(
        title = "Enable TLS",
        description = "Whether to use TLS/SSL for secure gRPC connection."
    )
    Property<Boolean> getTlsEnabled();
}
