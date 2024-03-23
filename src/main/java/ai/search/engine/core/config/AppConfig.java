package ai.search.engine.core.config;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.clip.CLIPModel;
import ai.search.engine.core.milvus.VectorDB;
import io.minio.MinioAsyncClient;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;
import lombok.SneakyThrows;

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
	@SneakyThrows
	public CLIPModel clipModel() {
		return new CLIPModel();
	}

	@Produces
	public ImageFactory  imageFactory() {
		return ImageFactory.getInstance();
	}
}
