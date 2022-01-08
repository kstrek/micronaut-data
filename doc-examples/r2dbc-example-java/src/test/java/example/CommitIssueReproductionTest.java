package example;

import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.r2dbc.operations.R2dbcOperations;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.Map;

@MicronautTest(transactional = false, rollback = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CommitIssueReproductionTest implements TestPropertyProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CommitIssueReproductionTest.class);

    static MySQLContainer<?> container;

    @Inject
    R2dbcOperations operations;

    @Inject
    AuthorRepository authorRepository;

    @AfterAll
    static void cleanup() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void reproduce() {
        Disposable disposable =
                Mono.from(operations.withTransaction(status ->
                        Mono.from(authorRepository.save(new Author("Stephen King")))
                )).doOnEach(signal -> {
                    if (signal.isOnComplete()) {
                        LOG.info("Transaction completed");
                    }
                }).subscribe();

        Awaitility.await().until(disposable::isDisposed);
    }

    @Override
    public Map<String, String> getProperties() {
        container = new MySQLContainer<>(DockerImageName.parse("mysql/mysql-server:8.0").asCompatibleSubstituteFor("mysql"));
        container.start();
        return CollectionUtils.mapOf(
                "datasources.default.url", container.getJdbcUrl(),
                "datasources.default.username", container.getUsername(),
                "datasources.default.password", container.getPassword(),
                "datasources.default.database", container.getDatabaseName(),
                "datasources.default.driverClassName", container.getDriverClassName(),
                "r2dbc.datasources.default.host", container.getHost(),
                "r2dbc.datasources.default.port", container.getFirstMappedPort(),
                "r2dbc.datasources.default.driver", "mysql",
                "r2dbc.datasources.default.username", container.getUsername(),
                "r2dbc.datasources.default.password", container.getPassword(),
                "r2dbc.datasources.default.database", container.getDatabaseName()
        );
    }
}
