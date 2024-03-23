package ai.search.engine.core.service;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.clip.CLIPModel;
import ai.search.engine.core.config.AppProperties;
import ai.search.engine.core.milvus.VectorDB;
import ai.search.engine.core.milvus.VectorDBCollection;
import ai.search.engine.core.milvus.VectorDBUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.json.Json;
import lombok.SneakyThrows;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ai.search.engine.core.milvus.VectorDBUtils.fieldType;

@ApplicationScoped
public class ImageDatabaseService {
	@Inject
	private AppProperties properties;
	@Inject
	private CLIPModel clipModel;
	@Inject
	private ImageFactory imageFactory;

	private VectorDB database;

	void onStart(@Observes StartupEvent ev) {
		initCollection();
	}

	@SneakyThrows
	void onStop(@Observes ShutdownEvent ev) {
		database.close();
	}

		@SneakyThrows
	public void insertImage(File file) {
		var img = imageFactory.fromInputStream(new FileInputStream(file));
		float[] imgFeatures = clipModel.extractImageFeatures(img);
		var products = database.getOrCreateCollection("products").await().indefinitely();
		insertImageOnDB(products, file, VectorDBUtils.embeddingToList(imgFeatures));
		products.flush().await().indefinitely();
	}

	@SneakyThrows
	public void insertImageBatch(List<File> files) {
		var products = database.getOrCreateCollection("products").await().indefinitely();
		List<String> paths = new ArrayList<>();
		List<List<Float>> embeddings = new ArrayList<>();
		for (File file : files) {
			paths.add(file.getName());
			var img = imageFactory.fromInputStream(new FileInputStream(file));
			embeddings.add(VectorDBUtils.embeddingToList(clipModel.extractImageFeatures(img)));
		}
		insertImagesOnDb(products, paths, embeddings);
		products.flush().await().indefinitely();
	}

	private void insertImagesOnDb(VectorDBCollection collection, List<String> paths, List<List<Float>> embeddings) {
		collection.insert(Map.of(
				"path", paths,
				"embedding", embeddings
		)).await().indefinitely();
	}

	private void insertImageOnDB(VectorDBCollection collection, File file, List<Float> embeddings) {
		collection.insert(Map.of(
				"path", List.of(file.getName()),
				"embedding", embeddings
		)).await().indefinitely();
	}

	private void initCollection() {
		database = VectorDB.getOrCreateDatabase(properties.dbUrl(), properties.dbToken(), properties.dbName())
				.await().indefinitely();
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
}
