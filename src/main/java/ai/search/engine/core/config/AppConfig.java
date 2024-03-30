package ai.search.engine.core.config;

import ai.djl.modality.cv.ImageFactory;
import io.minio.MinioAsyncClient;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;

@Dependent
public class AppConfig {

	@Produces
	public MinioAsyncClient minioClient() {
		return MinioAsyncClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("minioadmin", "minioadmin")
				.build();
	}

	@Produces
	public ImageFactory imageFactory() {
		return ImageFactory.getInstance();
	}
}
