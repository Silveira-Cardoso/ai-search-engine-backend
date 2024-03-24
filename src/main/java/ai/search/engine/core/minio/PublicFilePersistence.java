package ai.search.engine.core.minio;

import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class PublicFilePersistence extends FilePersistanceAbstract {
	@Inject
	protected PublicFilePersistence(MinioAsyncClient minioClient) {
		super("public", minioClient, true);
	}

}
