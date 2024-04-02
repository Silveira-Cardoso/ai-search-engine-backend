package ai.search.engine.core.minio;

import ai.search.engine.core.config.AppProperties;
import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class ImportFilePersistence extends FilePersistenceAbstract {

	@Inject
	protected ImportFilePersistence(MinioAsyncClient minioClient, AppProperties properties) {
		super(properties.bucketFrom(), minioClient, false, properties.embeddingsBatchSize());
	}
}
