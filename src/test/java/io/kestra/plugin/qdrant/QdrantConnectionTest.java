package io.kestra.plugin.qdrant;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class QdrantConnectionTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testToPointIdValid() {
        Common.PointId numId = QdrantConnection.toPointId(12345);
        assertThat(numId.getNum(), is(12345L));

        String uuidStr = UUID.randomUUID().toString();
        Common.PointId uuidId = QdrantConnection.toPointId(uuidStr);
        assertThat(uuidId.getUuid(), is(uuidStr));


    }

    @Test
    void testToPointIdInvalidFailsFast() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> QdrantConnection.toPointId("doc_123")
        );
        assertThat(ex.getMessage(), containsString("Invalid point ID 'doc_123'"));
    }

    @Test
    void testToFilterRangeUnderMust() {
        Map<String, Object> filterMap = Map.of(
            "must", List.of(
                Map.of(
                    "key", "price",
                    "range", Map.of("gte", 100)
                )
            )
        );

        Common.Filter filter = QdrantConnection.toFilter(filterMap);
        assertThat(filter, notNullValue());
        assertThat(filter.getMustCount(), is(1));

        Common.Condition cond = filter.getMust(0);
        assertThat(cond.hasField(), is(true));
        assertThat(cond.getField().getKey(), is("price"));
        assertThat(cond.getField().hasRange(), is(true));
        assertThat(cond.getField().getRange().getGte(), is(100.0));
    }

    @Test
    void testToFilterTopLevelCombinedWithMust() {
        Map<String, Object> filterMap = Map.of(
            "must", List.of(
                Map.of(
                    "key", "price",
                    "range", Map.of("gte", 100)
                )
            ),
            "city", "Berlin"
        );

        Common.Filter filter = QdrantConnection.toFilter(filterMap);
        assertThat(filter, notNullValue());
        // Both the condition under must and the top-level city condition should be present
        assertThat(filter.getMustCount(), is(2));
    }

    @Test
    void testPayloadMemoryToGrpc() {
        assertThat(PayloadMemory.COLD.toGrpc(), is(Collections.Memory.Cold));
        assertThat(PayloadMemory.CACHED.toGrpc(), is(Collections.Memory.Cached));
        assertThat(PayloadMemory.PINNED.toGrpc(), is(Collections.Memory.Pinned));
    }

    @Test
    void testStreamingStoreFetchOutput() throws IOException {
        RunContext runContext = runContextFactory.of();
        List<String> items = List.of("alpha", "beta");

        var output = QdrantConnection.buildFetchOutput(
            runContext,
            FetchType.STORE,
            items,
            item -> Map.of("name", item)
        );

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getUri(), notNullValue());
    }

    @Test
    void testToFilterKeyWithText() {
        Map<String, Object> filterMap = Map.of(
            "must", List.of(
                Map.of(
                    "key", "title",
                    "text", "kestra"
                )
            )
        );

        Common.Filter filter = QdrantConnection.toFilter(filterMap);
        assertThat(filter, notNullValue());
        assertThat(filter.getMustCount(), is(1));

        Common.Condition cond = filter.getMust(0);
        assertThat(cond.hasField(), is(true));
        assertThat(cond.getField().getKey(), is("title"));
        assertThat(cond.getField().hasMatch(), is(true));
        assertThat(cond.getField().getMatch().getText(), is("kestra"));
    }

    @Test
    void testToFilterMatchAnyAndExcept() {
        Map<String, Object> filterMap = Map.of(
            "must", List.of(
                Map.of(
                    "key", "city",
                    "match", Map.of("any", List.of("Berlin", "Paris"))
                ),
                Map.of(
                    "key", "status",
                    "match", Map.of("except", List.of("archived", "deleted"))
                ),
                Map.of(
                    "key", "tag_id",
                    "match", Map.of("any", List.of(1, 2, 3))
                ),
                Map.of(
                    "key", "exclude_id",
                    "match", Map.of("except", List.of(10, 20))
                )
            )
        );

        Common.Filter filter = QdrantConnection.toFilter(filterMap);
        assertThat(filter, notNullValue());
        assertThat(filter.getMustCount(), is(4));

        Common.Condition condAny = filter.getMust(0);
        assertThat(condAny.getField().getKey(), is("city"));
        assertThat(condAny.getField().getMatch().getKeywords().getStringsList(), is(List.of("Berlin", "Paris")));

        Common.Condition condExcept = filter.getMust(1);
        assertThat(condExcept.getField().getKey(), is("status"));
        assertThat(condExcept.getField().getMatch().getExceptKeywords().getStringsList(), is(List.of("archived", "deleted")));

        Common.Condition condAnyNum = filter.getMust(2);
        assertThat(condAnyNum.getField().getKey(), is("tag_id"));
        assertThat(condAnyNum.getField().getMatch().getIntegers().getIntegersList(), is(List.of(1L, 2L, 3L)));

        Common.Condition condExceptNum = filter.getMust(3);
        assertThat(condExceptNum.getField().getKey(), is("exclude_id"));
        assertThat(condExceptNum.getField().getMatch().getExceptIntegers().getIntegersList(), is(List.of(10L, 20L)));
    }

    @Test
    void testToFilterUnsupportedShapeFailsFast() {
        Map<String, Object> filterMap = Map.of(
            "location", Map.of("unsupported_op", Map.of("lat", 52.5, "lon", 13.4))
        );

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> QdrantConnection.toFilter(filterMap)
        );
        assertThat(ex.getMessage(), containsString("Unsupported filter condition for key 'location'"));
    }

    @Test
    void testFromVectorsOutputWithNamedVectors() {
        var denseText = Points.VectorOutput.newBuilder()
            .setDense(Points.DenseVector.newBuilder().addAllData(List.of(0.1f, 0.2f)).build())
            .build();
        var denseImage = Points.VectorOutput.newBuilder()
            .setDense(Points.DenseVector.newBuilder().addAllData(List.of(0.9f, 0.8f)).build())
            .build();

        var namedVectors = Points.NamedVectorsOutput.newBuilder()
            .putVectors("text", denseText)
            .putVectors("image", denseImage)
            .build();

        var vectorsOutput = Points.VectorsOutput.newBuilder()
            .setVectors(namedVectors)
            .build();

        Object result = QdrantConnection.fromVectorsOutput(vectorsOutput);
        assertThat(result, notNullValue());
        assertThat(result instanceof Map<?, ?>, is(true));

        @SuppressWarnings("unchecked")
        Map<String, List<Float>> map = (Map<String, List<Float>>) result;
        assertThat(map.get("text"), is(List.of(0.1f, 0.2f)));
        assertThat(map.get("image"), is(List.of(0.9f, 0.8f)));
    }

    @Test
    void testQueryConfigurationWithLimitAndVectorName() {
        Query query = Query.builder()
            .host(io.kestra.core.models.property.Property.ofValue("localhost"))
            .collectionName(io.kestra.core.models.property.Property.ofValue("test_col"))
            .vector(io.kestra.core.models.property.Property.ofValue(List.of(0.1f, 0.2f)))
            .limit(io.kestra.core.models.property.Property.ofValue(5))
            .vectorName(io.kestra.core.models.property.Property.ofValue("custom_vec"))
            .build();

        assertThat(query, notNullValue());
        assertThat(query.getLimit(), notNullValue());
        assertThat(query.getVectorName(), notNullValue());
    }

    @Test
    void testQueryLimitTakesPrecedenceOverTopK() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Both limit and topK provided: limit (5) wins over topK (20)
        Query queryBoth = Query.builder()
            .host(io.kestra.core.models.property.Property.ofValue("localhost"))
            .collectionName(io.kestra.core.models.property.Property.ofValue("test_col"))
            .vector(io.kestra.core.models.property.Property.ofValue(List.of(0.1f, 0.2f)))
            .limit(io.kestra.core.models.property.Property.ofValue(5))
            .topK(io.kestra.core.models.property.Property.ofValue(20))
            .build();
        assertThat(queryBoth.resolveLimit(runContext), is(5));

        // 2. Only topK provided: topK is used
        Query queryTopKOnly = Query.builder()
            .host(io.kestra.core.models.property.Property.ofValue("localhost"))
            .collectionName(io.kestra.core.models.property.Property.ofValue("test_col"))
            .vector(io.kestra.core.models.property.Property.ofValue(List.of(0.1f, 0.2f)))
            .topK(io.kestra.core.models.property.Property.ofValue(25))
            .build();
        assertThat(queryTopKOnly.resolveLimit(runContext), is(25));

        // 3. Neither provided: defaults to 10
        Query queryDefault = Query.builder()
            .host(io.kestra.core.models.property.Property.ofValue("localhost"))
            .collectionName(io.kestra.core.models.property.Property.ofValue("test_col"))
            .vector(io.kestra.core.models.property.Property.ofValue(List.of(0.1f, 0.2f)))
            .build();
        assertThat(queryDefault.resolveLimit(runContext), is(10));
    }
}
