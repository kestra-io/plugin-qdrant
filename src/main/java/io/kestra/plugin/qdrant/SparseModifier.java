package io.kestra.plugin.qdrant;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    title = "Sparse vector modifier",
    description = "Server-side adjustment applied to the weights of a sparse vector space."
)
public enum SparseModifier {
    @Schema(title = "Use the stored weights as they are")
    NONE,

    @Schema(title = "Multiply the weights by the inverse document frequency computed by Qdrant, as BM25 does")
    IDF;

    public io.qdrant.client.grpc.Collections.Modifier toGrpc() {
        return switch (this) {
            case NONE -> io.qdrant.client.grpc.Collections.Modifier.None;
            case IDF -> io.qdrant.client.grpc.Collections.Modifier.Idf;
        };
    }
}
