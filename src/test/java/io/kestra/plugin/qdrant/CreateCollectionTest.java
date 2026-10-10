package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CreateCollectionTest extends QdrantTest {

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var task = CreateCollection.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vectorSize(Property.ofValue(DIMENSION))
            .distance(Property.ofValue(Distance.COSINE))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCollectionName(), is(collectionName));
        assertThat(output.getSuccess(), is(true));
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
