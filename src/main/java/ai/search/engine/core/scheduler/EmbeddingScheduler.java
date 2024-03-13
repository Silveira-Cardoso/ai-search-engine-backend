package ai.search.engine.core.scheduler;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.config.AppConfig;
import ai.search.engine.core.model.CLIPModel;
import ai.search.engine.core.vectordb.VectorDB;
import ai.search.engine.core.vectordb.VectorDBUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.json.Json;
import lombok.extern.jbosslog.JBossLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static ai.search.engine.core.vectordb.VectorDBUtils.fieldType;

@JBossLog
@ApplicationScoped
public class EmbeddingScheduler {

	@Inject
	private AppConfig config;
	private VectorDB database;

	void onInit(@Observes @Initialized(ApplicationScoped.class) Object event) {
		LOG.info("Starting the embedding scheduler...");
		this.database = VectorDB.getOrCreateDatabase(config.getDbUrl(), config.getDbToken(), config.getDbName())
				.await().indefinitely();
		LOG.info("Creating products collection...");
		var products = database.getOrCreateCollection("products", List.of(
						fieldType("id", DataType.Int64,
								builder -> builder.withAutoID(true)
										.withPrimaryKey(true)),
						fieldType("type", DataType.VarChar,
								builder -> builder.withMaxLength(255)),
						fieldType("embedding", DataType.FloatVector,
								builder -> builder.withDimension(512))))
				.await().indefinitely();

		var indexParam = Json.createObjectBuilder()
				.add("nlist", 1024)
				.build();
		products.createIndexIfNotExists("embedding", "idx_embedding",
						indexParam, IndexType.IVF_FLAT, MetricType.COSINE)
				.await().indefinitely();
		products.load()
				.await().indefinitely();
	}

	void onClose(@Observes @Initialized(ApplicationScoped.class) Object event) throws InterruptedException {
		LOG.info("Stopping the embedding scheduler...");
		database.close();
	}

	@Scheduled(every = "10s")
	public void schedule() throws InterruptedException {
		LOG.info("Starting vector search...");

		var products = database.getOrCreateCollection("products")
				.await().indefinitely();

		var imageFactory = ImageFactory.getInstance();
		try (var model = new CLIPModel();
			 var imagesPath = Files.list(Paths.get(config.getImagesPath()))) {
			var embeddings = imagesPath.map(path -> {
						try {
							return imageFactory.fromFile(path);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					})
					.map(model::extractImageFeatures)
					.map(VectorDBUtils::embeddingToList)
					.toList();

			var count = products.insert(Map.of(
					"type", List.of("fashion"),
					"embedding", embeddings
			)).await().indefinitely();

			LOG.info("insert count: " + count);

			products.flush()
					.await().indefinitely();
		} catch (Throwable th) {
			LOG.error("Error: " + th.getMessage(), th);
		}
	}
}
