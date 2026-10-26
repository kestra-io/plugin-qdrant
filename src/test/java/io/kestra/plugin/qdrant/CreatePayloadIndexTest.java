package io.kestra.plugin.qdrant;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.qdrant.client.ConditionFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Common;
import io.qdrant.client.grpc.Points;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CreatePayloadIndexTest extends QdrantTest {
    @BeforeEach
    void createCollection() throws Exception {
        try (var client = qdrantClient()) {
            client.createCollectionAsync(collectionName, Collections.VectorParams.newBuilder()
                .setSize(DIMENSION).setDistance(Collections.Distance.Cosine).build()).get();
        }
    }

    @AfterEach
    void deleteCollection() throws Exception {
        try (var client = qdrantClient()) {
            client.deleteCollectionAsync(collectionName).get();
        }
    }

    private CreatePayloadIndex.CreatePayloadIndexBuilder<?, ?> task() {
        return CreatePayloadIndex.builder()
            .id(IdUtils.create()).type(CreatePayloadIndex.class.getName())
            .host(Property.ofValue(host)).port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .fieldName(Property.ofValue("tenant_id"))
            .fieldSchema(Property.ofValue(PayloadSchema.KEYWORD));
    }

    @Test
    void shouldCreateEverySupportedPayloadIndexType() throws Exception {
        var runContext = runContextFactory.of();
        for (var schema : PayloadSchema.values()) {
            var field = "field_" + schema.name().toLowerCase(java.util.Locale.ROOT);
            var output = task().fieldName(Property.ofValue(field)).fieldSchema(Property.ofValue(schema)).build().run(runContext);
            assertThat(output.getStatus(), is("Completed"));
            var info = CollectionInfo.builder()
                .host(Property.ofValue(host)).port(Property.ofValue(port))
                .collectionName(Property.ofValue(collectionName)).build().run(runContext);
            assertThat(info.getPayloadSchema().get(field).dataType(), is(schema.name().toLowerCase(java.util.Locale.ROOT)));
        }
    }

    @Test
    void shouldRenderFieldNameAndEnableStrictModeFilteringWithoutChangingPoints() throws Exception {
        var runContext = runContextFactory.of(Map.of("vars", Map.of("field", "tenant_id")));
        Upsert.builder()
            .host(Property.ofValue(host)).port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .points(List.of(Map.of("id", 1, "vector", List.of(1f, 0f, 0f, 0f), "payload", Map.of("tenant_id", "team"))))
            .build().run(runContext);
        try (var client = qdrantClient()) {
            client.updateCollectionAsync(Collections.UpdateCollection.newBuilder()
                .setCollectionName(collectionName)
                .setStrictModeConfig(Collections.StrictModeConfig.newBuilder()
                    .setEnabled(true).setUnindexedFilteringRetrieve(false).build())
                .build()).get();
            var request = Points.ScrollPoints.newBuilder()
                .setCollectionName(collectionName)
                .setFilter(Common.Filter.newBuilder().addMust(ConditionFactory.matchKeyword("tenant_id", "team")))
                .setLimit(10).build();
            assertThrows(ExecutionException.class, () -> client.scrollAsync(request).get());

            var output = task().fieldName(Property.ofExpression("{{ vars.field }}")).build().run(runContext);
            var points = client.scrollAsync(request).get();

            assertThat(output.getStatus(), is("Completed"));
            assertThat(points.getResultCount(), is(1));
            assertThat(points.getResult(0).getId().getNum(), is(1L));
        }
    }

    @Test
    void shouldAcknowledgeAnAsynchronousIndexRequest() throws Exception {
        var output = task().wait(Property.ofValue(false)).build().run(runContextFactory.of());

        assertThat(output.getStatus(), is("Acknowledged"));
    }

    @Test
    void shouldExplainMissingCollectionFailures() {
        var task = task().collectionName(Property.ofValue(collectionName + "_missing")).build();
        var error = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(error.getMessage(), containsString("Check the collection name"));
        assertThat(error.getMessage(), containsString(collectionName + "_missing"));
        assertThat(error.getCause(), notNullValue());
    }

    @Test
    void shouldRejectBlankNamesAndNonPositiveTimeouts() {
        var runContext = runContextFactory.of();
        assertThrows(IllegalArgumentException.class, () -> task().fieldName(Property.ofValue(" ")).build().run(runContext));
        assertThrows(IllegalArgumentException.class, () -> task().collectionName(Property.ofValue(" ")).build().run(runContext));
        assertThrows(IllegalArgumentException.class, () -> task().requestTimeout(Property.ofValue(Duration.ZERO)).build().run(runContext));
        assertThrows(IllegalArgumentException.class, () -> task().requestTimeout(Property.ofValue(Duration.ofSeconds(-1))).build().run(runContext));
    }
}
