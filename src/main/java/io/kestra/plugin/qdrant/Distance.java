package io.kestra.plugin.qdrant;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    title = "Distance metric",
    description = "Distance metric used for vector similarity comparison."
)
public enum Distance {
    @Schema(title = "Cosine similarity")
    COSINE,

    @Schema(title = "Dot product")
    DOT,

    @Schema(title = "Euclidean distance")
    EUCLID,

    @Schema(title = "Manhattan distance")
    MANHATTAN;

    public io.qdrant.client.grpc.Collections.Distance toGrpc() {
        return switch (this) {
            case COSINE -> io.qdrant.client.grpc.Collections.Distance.Cosine;
            case DOT -> io.qdrant.client.grpc.Collections.Distance.Dot;
            case EUCLID -> io.qdrant.client.grpc.Collections.Distance.Euclid;
            case MANHATTAN -> io.qdrant.client.grpc.Collections.Distance.Manhattan;
        };
    }
}
