package ai.search.engine.core.minio;

import ai.search.engine.core.config.AppProperties;
import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class PublicFilePersistence extends FilePersistenceAbstract {
	@Inject
	protected PublicFilePersistence(MinioAsyncClient minioClient, AppProperties properties) {
		super("public", minioClient, true, properties.embeddingsBatchSize());
	}
}
