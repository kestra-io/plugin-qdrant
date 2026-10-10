package io.kestra.plugin.qdrant;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.qdrant.client.ConditionFactory;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.grpc.Common;
import io.grpc.LoadBalancerRegistry;
import io.grpc.LoadBalancerProvider;
import io.qdrant.client.grpc.JsonWithInt;
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
import reactor.core.publisher.Flux;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class QdrantConnection extends Task implements QdrantConnectionInterface {

    @Schema(
        title = "The host of the Qdrant instance",
        description = "The hostname or IP address of the Qdrant gRPC endpoint."
    )
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> host;

    @Schema(
        title = "The port of the Qdrant instance",
        description = "The gRPC port of the Qdrant instance (default 6334)."
    )
    @Builder.Default
    @Min(1)
    @Max(65535)
    @PluginProperty(group = "connection")
    protected Property<Integer> port = Property.ofValue(6334);

    @Schema(
        title = "API key",
        description = "The API key for authentication with Qdrant."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<String> apiKey;

    @Schema(
        title = "Enable TLS",
        description = "Whether to use TLS/SSL for secure gRPC connection."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> tlsEnabled = Property.ofValue(false);

    public QdrantClient buildClient(RunContext runContext) throws IllegalVariableEvaluationException {
        ensurePickFirstRegistered();

        var rHost = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("'host' is required"));
        rHost = rHost.replaceFirst("^https?://", "");
        var rPort = runContext.render(this.port).as(Integer.class).orElse(6334);
        var rApiKey = runContext.render(this.apiKey).as(String.class).orElse(null);
        var rTlsEnabled = runContext.render(this.tlsEnabled).as(Boolean.class).orElse(false);

        QdrantGrpcClient.Builder builder = QdrantGrpcClient.newBuilder(rHost, rPort, rTlsEnabled);
        if (rApiKey != null && !rApiKey.isBlank()) {
            builder.withApiKey(rApiKey);
        }

        return new QdrantClient(builder.build());
    }

    private static void ensurePickFirstRegistered() {
        var registry = LoadBalancerRegistry.getDefaultRegistry();
        if (registry.getProvider("pick_first") != null) {
            return;
        }
        try {
            var providerClass = Class.forName(
                "io.grpc.internal.PickFirstLoadBalancerProvider",
                true,
                QdrantConnection.class.getClassLoader()
            );
            registry.register((LoadBalancerProvider) providerClass.getDeclaredConstructor().newInstance());
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(QdrantConnection.class)
                .warn("Failed to register pick_first gRPC load balancer provider (best-effort fallback covered by META-INF/services)", e);
        }
    }

    public static Common.PointId toPointId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Point ID cannot be null");
        }
        if (id instanceof Common.PointId pointId) {
            return pointId;
        }
        if (id instanceof Number number) {
            long val = number.longValue();
            if (val < 0) {
                throw new IllegalArgumentException("Point ID number must be a non-negative unsigned 64-bit integer, got: " + val);
            }
            return PointIdFactory.id(val);
        }
        if (id instanceof UUID uuid) {
            return PointIdFactory.id(uuid);
        }
        String str = id.toString();
        try {
            long val = Long.parseLong(str);
            if (val >= 0) {
                return PointIdFactory.id(val);
            }
        } catch (NumberFormatException ignored) {
        }
        try {
            return PointIdFactory.id(UUID.fromString(str));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid point ID '" + str + "': Qdrant requires an unsigned 64-bit integer or a valid RFC-4122 UUID.", e);
        }
    }

    public static Object fromPointId(Common.PointId pointId) {
        if (pointId == null) {
            return null;
        }
        return switch (pointId.getPointIdOptionsCase()) {
            case NUM -> pointId.getNum();
            case UUID -> pointId.getUuid();
            case POINTIDOPTIONS_NOT_SET -> null;
        };
    }

    public static JsonWithInt.Value toValue(Object obj) {
        if (obj == null) {
            return ValueFactory.nullValue();
        }
        if (obj instanceof Boolean b) {
            return ValueFactory.value(b);
        }
        if (obj instanceof Integer i) {
            return ValueFactory.value((long) i);
        }
        if (obj instanceof Long l) {
            return ValueFactory.value(l);
        }
        if (obj instanceof Short s) {
            return ValueFactory.value((long) s);
        }
        if (obj instanceof Byte b) {
            return ValueFactory.value((long) b);
        }
        if (obj instanceof Float f) {
            return ValueFactory.value((double) f);
        }
        if (obj instanceof Double d) {
            return ValueFactory.value(d);
        }
        if (obj instanceof Number n) {
            return ValueFactory.value(n.doubleValue());
        }
        if (obj instanceof String s) {
            return ValueFactory.value(s);
        }
        if (obj instanceof List<?> list) {
            List<JsonWithInt.Value> values = list.stream().map(QdrantConnection::toValue).toList();
            return ValueFactory.list(values);
        }
        if (obj instanceof Map<?, ?> map) {
            Map<String, JsonWithInt.Value> values = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                values.put(String.valueOf(entry.getKey()), toValue(entry.getValue()));
            }
            return ValueFactory.value(values);
        }
        return ValueFactory.value(obj.toString());
    }

    public static Object fromValue(JsonWithInt.Value value) {
        if (value == null) {
            return null;
        }
        return switch (value.getKindCase()) {
            case NULL_VALUE -> null;
            case BOOL_VALUE -> value.getBoolValue();
            case INTEGER_VALUE -> value.getIntegerValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case STRING_VALUE -> value.getStringValue();
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                .map(QdrantConnection::fromValue)
                .toList();
            case STRUCT_VALUE -> fromPayloadMap(value.getStructValue().getFieldsMap());
            case KIND_NOT_SET -> null;
        };
    }

    public static Map<String, JsonWithInt.Value> toPayloadMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, JsonWithInt.Value> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            result.put(entry.getKey(), toValue(entry.getValue()));
        }
        return result;
    }

    public static Map<String, Object> fromPayloadMap(Map<String, JsonWithInt.Value> map) {
        if (map == null || map.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, JsonWithInt.Value> entry : map.entrySet()) {
            result.put(entry.getKey(), fromValue(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static Common.Filter toFilter(Map<String, Object> filterMap) {
        if (filterMap == null || filterMap.isEmpty()) {
            return null;
        }

        Common.Filter.Builder filterBuilder = Common.Filter.newBuilder();

        if (filterMap.containsKey("must") || filterMap.containsKey("should") || filterMap.containsKey("must_not")) {
            if (filterMap.get("must") instanceof List<?> mustList) {
                for (Object item : mustList) {
                    if (item instanceof Map<?, ?> condMap) {
                        filterBuilder.addMust(mapToCondition((Map<String, Object>) condMap));
                    }
                }
            }
            if (filterMap.get("should") instanceof List<?> shouldList) {
                for (Object item : shouldList) {
                    if (item instanceof Map<?, ?> condMap) {
                        filterBuilder.addShould(mapToCondition((Map<String, Object>) condMap));
                    }
                }
            }
            if (filterMap.get("must_not") instanceof List<?> mustNotList) {
                for (Object item : mustNotList) {
                    if (item instanceof Map<?, ?> condMap) {
                        filterBuilder.addMustNot(mapToCondition((Map<String, Object>) condMap));
                    }
                }
            }
        }

        if (filterMap.containsKey("range") && filterMap.get("range") instanceof Map<?, ?> rangeFieldMap) {
            for (Map.Entry<?, ?> rEntry : rangeFieldMap.entrySet()) {
                var rKey = String.valueOf(rEntry.getKey());
                if (rEntry.getValue() instanceof Map<?, ?> rMap) {
                    Common.Range.Builder rb = Common.Range.newBuilder();
                    if (rMap.get("gte") instanceof Number n) rb.setGte(n.doubleValue());
                    if (rMap.get("gt") instanceof Number n) rb.setGt(n.doubleValue());
                    if (rMap.get("lte") instanceof Number n) rb.setLte(n.doubleValue());
                    if (rMap.get("lt") instanceof Number n) rb.setLt(n.doubleValue());
                    filterBuilder.addMust(ConditionFactory.range(rKey, rb.build()));
                }
            }
        }

        for (Map.Entry<String, Object> entry : filterMap.entrySet()) {
            if ("range".equals(entry.getKey()) || "must".equals(entry.getKey()) || "should".equals(entry.getKey()) || "must_not".equals(entry.getKey())) {
                continue;
            }
            filterBuilder.addMust(toSingleCondition(entry.getKey(), entry.getValue()));
        }
        return filterBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private static Common.Condition mapToCondition(Map<String, Object> map) {
        if (map.containsKey("key")) {
            String key = (String) map.get("key");
            if (map.containsKey("range")) {
                return toSingleCondition(key, map.get("range"));
            }
            if (map.containsKey("match")) {
                Object match = map.get("match");
                if (match instanceof Map<?, ?> matchMap && matchMap.containsKey("value")) {
                    return toSingleCondition(key, matchMap.get("value"));
                }
                return toSingleCondition(key, match);
            }
            if (map.containsKey("text")) {
                return ConditionFactory.matchText(key, map.get("text").toString());
            }
            if (map.containsKey("value")) {
                return toSingleCondition(key, map.get("value"));
            }
            if (map.containsKey("any")) {
                return toSingleCondition(key, Map.of("any", map.get("any")));
            }
            if (map.containsKey("except")) {
                return toSingleCondition(key, Map.of("except", map.get("except")));
            }
            if (map.containsKey("is_empty") || map.containsKey("is_null")) {
                return toSingleCondition(key, map);
            }
        }
        if (!map.isEmpty()) {
            var firstEntry = map.entrySet().iterator().next();
            return toSingleCondition(firstEntry.getKey(), firstEntry.getValue());
        }
        throw new IllegalArgumentException("Condition map cannot be empty");
    }

    private static Common.Condition toSingleCondition(String key, Object val) {
        if ("has_id".equalsIgnoreCase(key) || "id".equalsIgnoreCase(key)) {
            if (val instanceof List<?> idList) {
                List<Common.PointId> pids = idList.stream().map(QdrantConnection::toPointId).toList();
                return ConditionFactory.hasId(pids);
            } else if (val != null) {
                return ConditionFactory.hasId(toPointId(val));
            }
        }
        if (val instanceof Boolean b) {
            return ConditionFactory.match(key, b);
        } else if (val instanceof Integer i) {
            return ConditionFactory.match(key, (long) i);
        } else if (val instanceof Long l) {
            return ConditionFactory.match(key, l);
        } else if (val instanceof String s) {
            return ConditionFactory.matchKeyword(key, s);
        } else if (val instanceof List<?> list) {
            if (!list.isEmpty() && list.getFirst() instanceof Number) {
                List<Long> longs = list.stream().map(n -> ((Number) n).longValue()).toList();
                return ConditionFactory.matchValues(key, longs);
            } else {
                List<String> strings = list.stream().map(Object::toString).toList();
                return ConditionFactory.matchKeywords(key, strings);
            }
        } else if (val instanceof Map<?, ?> map) {
            if (map.containsKey("gte") || map.containsKey("gt") || map.containsKey("lte") || map.containsKey("lt")) {
                Common.Range.Builder rb = Common.Range.newBuilder();
                if (map.get("gte") instanceof Number n) rb.setGte(n.doubleValue());
                if (map.get("gt") instanceof Number n) rb.setGt(n.doubleValue());
                if (map.get("lte") instanceof Number n) rb.setLte(n.doubleValue());
                if (map.get("lt") instanceof Number n) rb.setLt(n.doubleValue());
                return ConditionFactory.range(key, rb.build());
            }
            if (map.containsKey("text")) {
                return ConditionFactory.matchText(key, map.get("text").toString());
            }
            if (map.containsKey("value")) {
                return toSingleCondition(key, map.get("value"));
            }
            if (map.containsKey("any") && map.get("any") instanceof List<?> list) {
                if (!list.isEmpty() && list.getFirst() instanceof Number) {
                    List<Long> longs = list.stream().map(n -> ((Number) n).longValue()).toList();
                    return ConditionFactory.matchValues(key, longs);
                } else {
                    List<String> strings = list.stream().map(Object::toString).toList();
                    return ConditionFactory.matchKeywords(key, strings);
                }
            }
            if (map.containsKey("except") && map.get("except") instanceof List<?> list) {
                if (!list.isEmpty() && list.getFirst() instanceof Number) {
                    List<Long> longs = list.stream().map(n -> ((Number) n).longValue()).toList();
                    return ConditionFactory.matchExceptValues(key, longs);
                } else {
                    List<String> strings = list.stream().map(Object::toString).toList();
                    return ConditionFactory.matchExceptKeywords(key, strings);
                }
            }
            if (Boolean.TRUE.equals(map.get("is_empty"))) {
                return ConditionFactory.isEmpty(key);
            }
            if (Boolean.TRUE.equals(map.get("is_null"))) {
                return ConditionFactory.isNull(key);
            }
            throw new IllegalArgumentException("Unsupported filter condition for key '" + key + "': " + map);
        } else if (val == null) {
            return ConditionFactory.isNull(key);
        } else {
            return ConditionFactory.matchKeyword(key, val.toString());
        }
    }

    public static URI store(RunContext runContext, List<Map<String, Object>> rows) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
            output.flush();
        }
        return runContext.storage().putFile(tempFile);
    }

    public static <T> FetchOutput buildFetchOutput(
        RunContext runContext,
        FetchType fetchType,
        List<T> items,
        Function<T, Map<String, Object>> mapper
    ) throws IOException {
        FetchOutput.FetchOutputBuilder builder = FetchOutput.builder();
        return switch (fetchType) {
            case FETCH_ONE -> builder
                .size(items.isEmpty() ? 0L : 1L)
                .row(items.isEmpty() ? null : mapper.apply(items.getFirst()))
                .build();
            case FETCH -> {
                List<Object> rows = new ArrayList<>(items.size());
                for (T item : items) {
                    rows.add(mapper.apply(item));
                }
                yield builder
                    .size((long) rows.size())
                    .rows(rows)
                    .build();
            }
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (OutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                    for (T item : items) {
                        FileSerde.write(output, mapper.apply(item));
                    }
                    output.flush();
                }
                yield builder
                    .size((long) items.size())
                    .uri(runContext.storage().putFile(tempFile))
                    .build();
            }
            default -> builder
                .size((long) items.size())
                .build();
        };
    }

    public static FetchOutput buildFetchOutput(RunContext runContext, FetchType fetchType, List<Map<String, Object>> rows) throws IOException {
        return buildFetchOutput(runContext, fetchType, rows, Function.identity());
    }

    public static List<Float> extractVectorData(Points.VectorOutput vector) {
        if (vector == null) {
            return Collections.emptyList();
        }
        if (vector.hasDense()) {
            return vector.getDense().getDataList();
        }
        if (vector.getDataCount() > 0) {
            return vector.getDataList();
        }
        return Collections.emptyList();
    }

    public static Object fromVectorsOutput(Points.VectorsOutput vectorsOutput) {
        if (vectorsOutput == null) {
            return null;
        }
        if (vectorsOutput.hasVector()) {
            return extractVectorData(vectorsOutput.getVector());
        }
        if (vectorsOutput.hasVectors()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<String, Points.VectorOutput> entry : vectorsOutput.getVectors().getVectorsMap().entrySet()) {
                map.put(entry.getKey(), extractVectorData(entry.getValue()));
            }
            return map;
        }
        return null;
    }
}
