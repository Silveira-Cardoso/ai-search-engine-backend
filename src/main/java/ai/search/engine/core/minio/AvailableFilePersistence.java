package ai.search.engine.core.minio;

import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class AvailableFilePersistence extends FilePersistanceAbstract{
	@Inject
	protected AvailableFilePersistence(MinioAsyncClient minioClient) {
		super("available", minioClient);
	}
}
