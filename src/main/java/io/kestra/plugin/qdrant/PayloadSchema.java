package io.kestra.plugin.qdrant;

import io.qdrant.client.grpc.Collections;

public enum PayloadSchema {
    KEYWORD, INTEGER, FLOAT, GEO, TEXT, BOOL, DATETIME, UUID;

    public Collections.PayloadSchemaType toGrpc() {
        return switch (this) {
            case KEYWORD -> Collections.PayloadSchemaType.Keyword;
            case INTEGER -> Collections.PayloadSchemaType.Integer;
            case FLOAT -> Collections.PayloadSchemaType.Float;
            case GEO -> Collections.PayloadSchemaType.Geo;
            case TEXT -> Collections.PayloadSchemaType.Text;
            case BOOL -> Collections.PayloadSchemaType.Bool;
            case DATETIME -> Collections.PayloadSchemaType.Datetime;
            case UUID -> Collections.PayloadSchemaType.Uuid;
        };
    }
}
