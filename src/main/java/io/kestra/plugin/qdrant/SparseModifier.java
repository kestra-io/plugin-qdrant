package io.kestra.plugin.qdrant;

import io.qdrant.client.grpc.Collections;

public enum SparseModifier {
    NONE,
    IDF;

    public Collections.Modifier toGrpc() {
        return switch (this) {
            case NONE -> Collections.Modifier.None;
            case IDF -> Collections.Modifier.Idf;
        };
    }
}
