package io.kestra.plugin.qdrant;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.qdrant.QdrantContainer;

@KestraTest
@Testcontainers(disabledWithoutDocker = true)
public abstract class QdrantTest {

    protected static final String API_KEY = "";
    protected static final int DIMENSION = 4;

    @Container
    protected static QdrantContainer qdrantContainer = new QdrantContainer("qdrant/qdrant:v1.19.1");

    protected static String host;
    protected static int port;

    @Inject
    protected RunContextFactory runContextFactory;

    protected final String collectionName = "kestra_" + getClass().getSimpleName().toLowerCase() + "_" + Long.toHexString(System.nanoTime());

    @BeforeAll
    public static void startQdrant() {
        if (qdrantContainer != null && !qdrantContainer.isRunning()) {
            try {
                qdrantContainer.start();
            } catch (Throwable t) {
                // Docker unavailable
            }
        }
        if (qdrantContainer != null && qdrantContainer.isRunning()) {
            host = qdrantContainer.getHost();
            port = qdrantContainer.getMappedPort(6334);
        }
    }

    protected boolean isQdrantAvailable() {
        return qdrantContainer != null && qdrantContainer.isRunning();
    }

    protected QdrantClient qdrantClient() {
        return new QdrantClient(QdrantGrpcClient.newBuilder(host, port, false).build());
    }
}
