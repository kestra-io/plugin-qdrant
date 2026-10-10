package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.qdrant.client.PointIdFactory;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Points;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryTest extends QdrantTest {

    @BeforeEach
    void setUpCollection() throws Exception {
        if (!isQdrantAvailable()) return;
        var vectorParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();
        try (var client = qdrantClient()) {
            client.createCollectionAsync(collectionName, vectorParams).get();

            var p1 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(1L))
                .setVectors(VectorsFactory.vectors(List.of(0.05f, 0.61f, 0.76f, 0.74f)))
                .putPayload("city", ValueFactory.value("Berlin"))
                .build();
            var p2 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(2L))
                .setVectors(VectorsFactory.vectors(List.of(0.19f, 0.81f, 0.75f, 0.11f)))
                .putPayload("city", ValueFactory.value("Paris"))
                .build();
            client.upsertAsync(collectionName, List.of(p1, p2)).get();
        }
    }

    @Test
    void runQueryVector() throws Exception {
        var runContext = runContextFactory.of();

        var task = Query.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vector(Property.ofValue(List.of(0.05f, 0.61f, 0.76f, 0.74f)))
            .topK(Property.ofValue(2))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), notNullValue());
        assertThat(output.getRows().size(), is(2));
    }

    @Test
    void runQueryWithLimit() throws Exception {
        var runContext = runContextFactory.of();

        var task = Query.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vector(Property.ofValue(List.of(0.05f, 0.61f, 0.76f, 0.74f)))
            .limit(Property.ofValue(2))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), notNullValue());
        assertThat(output.getRows().size(), is(2));
    }

    @Test
    void runQueryById() throws Exception {
        var runContext = runContextFactory.of();

        var task = Query.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vectorId(Property.ofValue(1))
            .topK(Property.ofValue(1))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow(), notNullValue());
    }

    @Test
    void runQueryWithNamedVector() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();
        String namedCol = collectionName + "_named";

        var vectorParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();
        var vectorParamsMap = Collections.VectorParamsMap.newBuilder()
            .putMap("text_vector", vectorParams)
            .build();

        try (var client = qdrantClient()) {
            client.createCollectionAsync(
                Collections.CreateCollection.newBuilder()
                    .setCollectionName(namedCol)
                    .setVectorsConfig(Collections.VectorsConfig.newBuilder().setParamsMap(vectorParamsMap).build())
                    .build()
            ).get();

            var p1 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(100L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    "text_vector", Points.Vector.newBuilder().addAllData(List.of(0.05f, 0.61f, 0.76f, 0.74f)).build()
                )))
                .putPayload("city", ValueFactory.value("Rome"))
                .build();
            client.upsertAsync(namedCol, List.of(p1)).get();

            // Test 1: Query with explicit vectorName
            var queryTask = Query.builder()
                .host(Property.ofValue(host))
                .port(Property.ofValue(port))
                .collectionName(Property.ofValue(namedCol))
                .vector(Property.ofValue(List.of(0.05f, 0.61f, 0.76f, 0.74f)))
                .vectorName(Property.ofValue("text_vector"))
                .limit(Property.ofValue(1))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .build();

            var output = queryTask.run(runContext);
            assertThat(output, notNullValue());
            assertThat(output.getSize(), is(1L));
            assertThat(output.getRow().get("id"), is(100L));

            // Test 2: Query by vectorId on point with single named vector without specifying vectorName (auto-detected)
            var queryByIdTask = Query.builder()
                .host(Property.ofValue(host))
                .port(Property.ofValue(port))
                .collectionName(Property.ofValue(namedCol))
                .vectorId(Property.ofValue(100L))
                .limit(Property.ofValue(1))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .build();

            var outputById = queryByIdTask.run(runContext);
            assertThat(outputById, notNullValue());
            assertThat(outputById.getSize(), is(1L));
            assertThat(outputById.getRow().get("id"), is(100L));

            client.deleteCollectionAsync(namedCol).get();
        }
    }

    @Test
    void runQueryWithMultiNamedVectors() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();
        String multiCol = collectionName + "_multi";

        var vectorParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();
        var vectorParamsMap = Collections.VectorParamsMap.newBuilder()
            .putMap("text_vector", vectorParams)
            .putMap("image_vector", vectorParams)
            .build();

        try (var client = qdrantClient()) {
            client.createCollectionAsync(
                Collections.CreateCollection.newBuilder()
                    .setCollectionName(multiCol)
                    .setVectorsConfig(Collections.VectorsConfig.newBuilder().setParamsMap(vectorParamsMap).build())
                    .build()
            ).get();

            var p1 = Points.PointStruct.newBuilder()
                .setId(PointIdFactory.id(200L))
                .setVectors(VectorsFactory.namedVectors(Map.of(
                    "text_vector", Points.Vector.newBuilder().addAllData(List.of(0.05f, 0.61f, 0.76f, 0.74f)).build(),
                    "image_vector", Points.Vector.newBuilder().addAllData(List.of(0.99f, 0.01f, 0.02f, 0.03f)).build()
                )))
                .putPayload("city", ValueFactory.value("Milan"))
                .build();
            client.upsertAsync(multiCol, List.of(p1)).get();

            // Test 1: Query selecting specific named vector from multi-vector point
            var queryTask = Query.builder()
                .host(Property.ofValue(host))
                .port(Property.ofValue(port))
                .collectionName(Property.ofValue(multiCol))
                .vectorId(Property.ofValue(200L))
                .vectorName(Property.ofValue("image_vector"))
                .limit(Property.ofValue(1))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .build();

            var output = queryTask.run(runContext);
            assertThat(output, notNullValue());
            assertThat(output.getSize(), is(1L));
            assertThat(output.getRow().get("id"), is(200L));

            // Test 2: Omitting vectorName on multi-vector point must throw clear IllegalArgumentException
            var ambiguousQueryTask = Query.builder()
                .host(Property.ofValue(host))
                .port(Property.ofValue(port))
                .collectionName(Property.ofValue(multiCol))
                .vectorId(Property.ofValue(200L))
                .limit(Property.ofValue(1))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .build();

            var ex = assertThrows(IllegalArgumentException.class, () -> ambiguousQueryTask.run(runContext));
            assertThat(ex.getMessage(), containsString("multiple named vectors"));

            client.deleteCollectionAsync(multiCol).get();
        }
    }

    @AfterEach
    void tearDown() {
        if (!isQdrantAvailable()) return;
        try (var client = qdrantClient()) {
            client.deleteCollectionAsync(collectionName).get();
        } catch (Exception ignored) {
        }
    }
}
