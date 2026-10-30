package io.kestra.plugin.qdrant;

import io.qdrant.client.grpc.Points;

public enum FusionMethod {
    RRF,
    DBSF;

    public Points.Fusion toGrpc() {
        return switch (this) {
            case RRF -> Points.Fusion.RRF;
            case DBSF -> Points.Fusion.DBSF;
        };
    }
}
