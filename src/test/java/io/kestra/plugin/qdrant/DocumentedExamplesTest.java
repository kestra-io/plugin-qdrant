package io.kestra.plugin.qdrant;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.core.log.Log;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

// A documented example that does not even parse or render is a broken copy-paste starting point, so every one is checked.
@KestraTest
public class DocumentedExamplesTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void everyDocumentedExampleIsAValidFlow() throws Exception {
        var checked = 0;
        for (var type : List.of(HybridQuery.class, SparseQuery.class, CreateCollection.class, Upsert.class)) {
            for (var example : type.getAnnotation(Plugin.class).examples()) {
                var yaml = yamlOf(example);
                if (!example.full()) {
                    continue;
                }
                // fails on unknown properties, wrong types and invalid bounds of any task of the example
                var flow = GenericFlow.fromYaml(TenantService.MAIN_TENANT, yaml);
                assertThat(type.getSimpleName() + ": " + flow.getId(), flow.getId().isBlank(), is(false));
                checked++;
            }
        }
        assertThat(checked, is(not(0)));
    }

    @Test
    void theComparisonExampleRendersItsLogMessageOutsideAnyLoop() throws Exception {
        var example = Arrays.stream(HybridQuery.class.getAnnotation(Plugin.class).examples())
            .filter(candidate -> yamlOf(candidate).contains("id: qdrant_hybrid_query_comparison"))
            .findFirst()
            .orElseThrow();
        var flow = YamlParser.parse(yamlOf(example), Flow.class);

        var compare = flow.getTasks().stream().filter(task -> "compare".equals(task.getId())).findFirst().orElseThrow();
        var message = String.valueOf(((Log) compare).getMessage());

        // this task is not inside a ForEach, so there is no taskrun.value to index the output with
        assertThat(message.contains("taskrun.value"), is(false));

        // and the expression renders against plain top level outputs
        var runContext = runContextFactory.of(Map.of(
            "outputs", Map.of(
                "dense_only", Map.of("rows", List.of(Map.of("id", 1), Map.of("id", 2))),
                "hybrid", Map.of("uri", "kestra:///fused.ion")
            )
        ));
        var rendered = runContext.render(message);
        assertThat(rendered, containsString("dense-only ids: [[1,2]]"));
        assertThat(rendered, containsString("fused ranking stored at: kestra:///fused.ion"));
    }

    private static String yamlOf(Example example) {
        return String.join("\n", example.code());
    }
}
