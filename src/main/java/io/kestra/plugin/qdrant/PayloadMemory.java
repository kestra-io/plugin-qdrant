package io.kestra.plugin.qdrant;

import io.qdrant.client.grpc.Collections;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Storage memory policy for point payload metadata in Qdrant (1.19+).
 */
@Schema(
    title = "Payload memory policy",
    description = "Storage memory tier for metadata payload."
)
public enum PayloadMemory {
    @Schema(title = "Stored on disk (mmap)")
    COLD,

    @Schema(title = "Stored on disk with in-memory page cache")
    CACHED,

    @Schema(title = "Pinned in RAM")
    PINNED;

    public Collections.Memory toGrpc() {
        return switch (this) {
            case COLD -> Collections.Memory.Cold;
            case CACHED -> Collections.Memory.Cached;
            case PINNED -> Collections.Memory.Pinned;
        };
    }
}
