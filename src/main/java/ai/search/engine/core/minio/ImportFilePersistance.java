package ai.search.engine.core.minio;

import io.minio.MinioAsyncClient;
import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Startup
public class ImportFilePersistance extends FilePersistanceAbstract{
	@Inject
	protected ImportFilePersistance(MinioAsyncClient minioClient) {
		super("import", minioClient, false);
	}
}
