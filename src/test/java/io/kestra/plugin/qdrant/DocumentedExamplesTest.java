package io.kestra.plugin.qdrant;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.YamlParser;
import io.kestra.plugin.core.log.Log;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

// A documented example that is not a valid flow is a broken copy-paste starting point, so every one is checked.
@KestraTest
public class DocumentedExamplesTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Test
    void everyDocumentedExampleIsAValidTypedFlow() throws Exception {
        var checked = 0;
        for (var type : List.of(HybridQuery.class, SparseQuery.class, CreateCollection.class, Upsert.class)) {
            for (var example : type.getAnnotation(Plugin.class).examples()) {
                if (!example.full()) {
                    continue;
                }

                // typed parsing fails on an unknown property or a wrong type of any task of the example
                var flow = YamlParser.parse(yamlOf(example), Flow.class);

                // and validation applies the @Min / @Max bounds to literal values
                var violations = modelValidator.isValid(flow);
                assertThat(type.getSimpleName() + " example '" + flow.getId() + "': " + violations, violations.isPresent(), is(false));

                // a literal of a Property field is only converted and bounded when it is rendered, so render each one
                for (var task : flow.allTasksWithChilds()) {
                    if (task instanceof QdrantConnection) {
                        renderLiterals(flow.getId(), task);
                    }
                }
                checked++;
            }
        }
        assertThat(checked, greaterThan(0));
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
        assertThat(rendered, containsString("dense-only ids: [1,2]"));
        assertThat(rendered, containsString("fused ranking stored at: kestra:///fused.ion"));
    }

    // renders every literal Property of simple type, so a wrong type or an out of range value fails the test
    private void renderLiterals(String flowId, Task task) throws Exception {
        var runContext = runContextFactory.of();
        for (Class<?> type = task.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (var field : type.getDeclaredFields()) {
                if (!Property.class.isAssignableFrom(field.getType())
                    || !(field.getGenericType() instanceof ParameterizedType generic)) {
                    continue;
                }

                var target = simpleTarget(generic.getActualTypeArguments()[0]);
                field.setAccessible(true);
                var property = (Property<?>) field.get(task);
                if (target == null || property == null || String.valueOf(property).contains("{{")) {
                    continue;
                }

                try {
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    var rendered = runContext.render((Property) property).as((Class) target).orElseThrow();

                    // Kestra checks @Min / @Max of the type argument on a typed value only, not on a YAML literal,
                    // so the converted value is put back into the task to let the validator apply the bounds
                    field.set(task, Property.ofValue(rendered));
                    var violations = modelValidator.isValid(task);
                    assertThat(String.valueOf(violations), violations.isPresent(), is(false));
                    field.set(task, property);
                } catch (Exception e) {
                    throw new AssertionError(
                        "example '" + flowId + "': " + task.getClass().getSimpleName() + "." + field.getName()
                            + " = " + property + " is not a valid " + target.getSimpleName() + ": " + e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    // only types that a YAML literal can be converted to without help of the flow context
    private static Class<?> simpleTarget(Type argument) {
        if (argument instanceof Class<?> type
            && (type == Integer.class || type == Float.class || type == Boolean.class || type == Duration.class || type.isEnum())) {
            return type;
        }
        return null;
    }

    private static String yamlOf(Example example) {
        return String.join("\n", example.code());
    }
}
