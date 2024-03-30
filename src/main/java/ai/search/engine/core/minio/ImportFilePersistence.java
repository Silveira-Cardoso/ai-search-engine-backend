package ai.search.engine.core.minio;

import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class ImportFilePersistence extends FilePersistenceAbstract {
	@Inject
	protected ImportFilePersistence(MinioAsyncClient minioClient) {
		super("import", minioClient, false, batchSize);
	}
}
