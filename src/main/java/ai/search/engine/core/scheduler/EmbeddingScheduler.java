package ai.search.engine.core.scheduler;

import ai.djl.ModelException;
import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.config.AppConfig;
import ai.search.engine.core.model.CLIPModel;
import ai.search.engine.core.vectordb.VectorDB;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.json.Json;
import lombok.extern.jbosslog.JBossLog;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static ai.search.engine.core.vectordb.VectorDBUtils.embeddingToList;
import static ai.search.engine.core.vectordb.VectorDBUtils.fieldType;

@Startup
@JBossLog
@ApplicationScoped
public class EmbeddingScheduler {

	@Inject
	AppConfig config;
	VectorDB database;
	CLIPModel model;

	void onStart(@Observes StartupEvent ev) throws ModelException, IOException {
		LOG.info("Starting the embedding scheduler...");
		this.database = VectorDB.getOrCreateDatabase(config.dbUrl(), config.dbToken(), config.dbName())
				.await().indefinitely();
		this.model = new CLIPModel();
		LOG.info("Creating products collection...");
		var products = database.getOrCreateCollection("products", List.of(
						fieldType("id", DataType.Int64,
								builder -> builder.withPrimaryKey(true)
										.withAutoID(true)),
						fieldType("path", DataType.VarChar,
								builder -> builder.withMaxLength(2048)),
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

	void onStop(@Observes ShutdownEvent ev) throws InterruptedException {
		LOG.info("Stopping the embedding scheduler...");
		database.close();
		model.close();
	}

	@Scheduled(every = "10s", concurrentExecution=Scheduled.ConcurrentExecution.SKIP)
	public void schedule() {
		LOG.info("Starting vector search...");

		var products = database.getOrCreateCollection("products")
				.await().indefinitely();

		var imageFactory = ImageFactory.getInstance();
		try (var imagesPath = Files.list(Paths.get(config.imagesPathFrom()))) {
			var pathsAndEmbeddings = imagesPath.map(path -> {
						try {
							var img = imageFactory.fromFile(path);
							var embedding = model.extractImageFeatures(img);
							return Pair.of(path.toAbsolutePath(),
									embeddingToList(embedding));
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					})
					.limit(config.embeddingsBatchSize())
					.toList();

			var paths = pathsAndEmbeddings
					.stream()
					.map(Pair::getLeft)
					.toList();
			var embeddings = pathsAndEmbeddings
					.stream()
					.map(Pair::getRight)
					.toList();

			var count = products.insert(Map.of(
					"path", paths.stream()
							.map(Path::toString)
							.toList(),
					"embedding", embeddings
			)).await().indefinitely();

			LOG.info("insert count: " + count);

			products.flush()
					.await().indefinitely();

			paths.forEach(to -> {
                try {
					var from = Paths.get(config.imagesPathTo()).resolve(to.getFileName());
                    Files.move(to, from);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
		} catch (Throwable th) {
			LOG.error("Error: " + th.getMessage(), th);
		}
	}
}
