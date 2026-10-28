package io.kestra.plugin.qdrant;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    title = "Fusion method",
    description = "How Qdrant merges the candidate lists of the prefetch branches into one ranking."
)
public enum FusionMethod {
    @Schema(title = "Reciprocal Rank Fusion, which scores a point from its rank in each branch")
    RRF,

    @Schema(title = "Distribution-Based Score Fusion, which normalizes each branch score distribution before summing")
    DBSF;

    public io.qdrant.client.grpc.Points.Fusion toGrpc() {
        return switch (this) {
            case RRF -> io.qdrant.client.grpc.Points.Fusion.RRF;
            case DBSF -> io.qdrant.client.grpc.Points.Fusion.DBSF;
        };
    }
}
