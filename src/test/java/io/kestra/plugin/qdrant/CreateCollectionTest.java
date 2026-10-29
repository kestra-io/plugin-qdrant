package io.kestra.plugin.qdrant;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import jakarta.inject.Inject;
import io.qdrant.client.grpc.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateCollectionTest extends QdrantTest {

    @Inject
    private ModelValidator modelValidator;

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

    @Test
    void createsNamedDenseAndSparseVectorSpaces() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        var output = base()
            .vectorName(Property.ofValue("text_dense"))
            .sparseVectors(List.of(
                CreateCollection.SparseVectorSpace.builder()
                    .name(Property.ofValue("text_sparse"))
                    .modifier(Property.ofValue(SparseModifier.IDF))
                    .memory(Property.ofValue(PayloadMemory.CACHED))
                    .fullScanThreshold(Property.ofValue(1000))
                    .build(),
                CreateCollection.SparseVectorSpace.builder()
                    .name(Property.ofValue("keywords"))
                    .build()
            ))
            .build()
            .run(runContext);

        assertThat(output.getSuccess(), is(true));

        try (var client = qdrantClient()) {
            var params = client.getCollectionInfoAsync(collectionName).get().getConfig().getParams();

            var dense = params.getVectorsConfig().getParamsMap().getMapMap().get("text_dense");
            assertThat(dense.getSize(), is((long) DIMENSION));
            assertThat(dense.getDistance(), is(Collections.Distance.Cosine));
            assertThat(params.getVectorsConfig().hasParams(), is(false));

            var sparse = params.getSparseVectorsConfig().getMapMap();
            assertThat(sparse.keySet(), is(Set.of("text_sparse", "keywords")));
            assertThat(sparse.get("text_sparse").getModifier(), is(Collections.Modifier.Idf));
            assertThat(sparse.get("text_sparse").getIndex().getMemory(), is(Collections.Memory.Cached));
            assertThat(sparse.get("text_sparse").getIndex().getFullScanThreshold(), is(1000L));
            assertThat(sparse.get("keywords").getModifier(), is(Collections.Modifier.None));
            assertThat(sparse.get("keywords").getIndex().hasMemory(), is(false));
        }
    }

    @Test
    void sparseVectorsWorkNextToTheUnnamedDenseVector() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        base()
            .sparseVectors(List.of(CreateCollection.SparseVectorSpace.builder().name(Property.ofValue("text_sparse")).build()))
            .build()
            .run(runContext);

        try (var client = qdrantClient()) {
            var params = client.getCollectionInfoAsync(collectionName).get().getConfig().getParams();

            assertThat(params.getVectorsConfig().hasParams(), is(true));
            assertThat(params.getSparseVectorsConfig().getMapMap().keySet(), is(Set.of("text_sparse")));
        }
    }

    @Test
    void withoutTheNewPropertiesTheCollectionStaysASingleUnnamedVector() throws Exception {
        if (!isQdrantAvailable()) return;
        var runContext = runContextFactory.of();

        base().build().run(runContext);

        try (var client = qdrantClient()) {
            var params = client.getCollectionInfoAsync(collectionName).get().getConfig().getParams();

            assertThat(params.getVectorsConfig().hasParams(), is(true));
            assertThat(params.getSparseVectorsConfig().getMapMap().isEmpty(), is(true));
        }
    }

    @Test
    void invalidSparseVectorDeclarationsAreRejectedBeforeAnyCall() {
        var runContext = runContextFactory.of();

        var sameAsDense = base().vectorName(Property.ofValue("v"))
            .sparseVectors(List.of(space("v"))).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> sameAsDense.run(runContext)).getMessage(),
            containsString("same name as the dense vector")
        );

        var duplicated = base().sparseVectors(List.of(space("s"), space("s"))).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> duplicated.run(runContext)).getMessage(),
            containsString("declared twice")
        );

        var blank = base().sparseVectors(List.of(space(" "))).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> blank.run(runContext)).getMessage(),
            containsString("must not be blank")
        );

        var unnamed = base().sparseVectors(List.of(CreateCollection.SparseVectorSpace.builder().build())).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> unnamed.run(runContext)).getMessage(),
            containsString("needs a 'name'")
        );

        var blankDense = base().vectorName(Property.ofValue("")).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> blankDense.run(runContext)).getMessage(),
            containsString("'vectorName' must not be blank")
        );

        var negativeThreshold = base().sparseVectors(List.of(
            CreateCollection.SparseVectorSpace.builder().name(Property.ofValue("s")).fullScanThreshold(Property.ofValue(-1)).build()
        )).build();
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> negativeThreshold.run(runContext)).getMessage(),
            containsString("fullScanThreshold' must be between 0")
        );
    }

    @Test
    void aNegativeFullScanThresholdIsRejectedByFlowValidation() {
        var invalid = base().id("create").type(CreateCollection.class.getName())
            .sparseVectors(List.of(
                CreateCollection.SparseVectorSpace.builder().name(Property.ofValue("s")).fullScanThreshold(Property.ofValue(-1)).build()
            ))
            .build();
        var valid = base().id("create").type(CreateCollection.class.getName())
            .sparseVectors(List.of(
                CreateCollection.SparseVectorSpace.builder().name(Property.ofValue("s")).fullScanThreshold(Property.ofValue(0)).build()
            ))
            .build();

        // rejected at validation time, before any execution
        assertThat(modelValidator.isValid(invalid).isPresent(), is(true));
        assertThat(modelValidator.isValid(valid).isPresent(), is(false));
    }

    private CreateCollection.CreateCollectionBuilder<?, ?> base() {
        return CreateCollection.builder()
            .host(Property.ofValue(host))
            .port(Property.ofValue(port))
            .collectionName(Property.ofValue(collectionName))
            .vectorSize(Property.ofValue(DIMENSION))
            .distance(Property.ofValue(Distance.COSINE));
    }

    private static CreateCollection.SparseVectorSpace space(String name) {
        return CreateCollection.SparseVectorSpace.builder().name(Property.ofValue(name)).build();
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
