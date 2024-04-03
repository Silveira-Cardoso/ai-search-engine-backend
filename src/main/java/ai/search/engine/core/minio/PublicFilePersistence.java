package ai.search.engine.core.minio;

import ai.search.engine.core.config.AppProperties;
import ai.search.engine.core.service.ByteArrayService;
import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class PublicFilePersistence extends FilePersistenceAbstract {

	@Inject
	protected PublicFilePersistence(MinioAsyncClient minioClient, ByteArrayService byteArrayService, AppProperties properties) {
		super(properties.bucketFrom(), minioClient, byteArrayService,true, properties.embeddingsBatchSize());
	}
}
