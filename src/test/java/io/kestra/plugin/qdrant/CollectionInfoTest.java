package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.qdrant.client.grpc.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CollectionInfoTest extends QdrantTest {

    @BeforeEach
    void setUpCollection() throws Exception {
        if (!isQdrantAvailable()) return;
        var vectorParams = Collections.VectorParams.newBuilder()
            .setSize(DIMENSION)
            .setDistance(Collections.Distance.Cosine)
            .build();
        try (var client = qdrantClient()) {
            client.createCollectionAsync(collectionName, vectorParams).get();
        }
    }

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var task = CollectionInfo.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCollectionName(), is(collectionName));
        assertThat(output.getStatus(), notNullValue());
        assertThat(output.getVectorSize(), is((long) DIMENSION));
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
