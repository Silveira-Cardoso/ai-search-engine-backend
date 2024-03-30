package ai.search.engine.core.milvus;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.clip.CLIPModel;
import ai.search.engine.core.config.ModelConfig;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import jakarta.json.Json;
import lombok.extern.jbosslog.JBossLog;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static ai.search.engine.core.milvus.VectorDBUtils.embeddingToList;

@JBossLog
public class VectorDBMainExample {

	public static void main(String[] args) {
		LOG.info("Starting vector search...");
		var databaseName = "aisearch";
		VectorDB database = VectorDB.getOrCreateDatabase("http://localhost:19530", "root:Milvus", databaseName)
				.await().indefinitely();
		VectorDBCollection products = database.getOrCreateCollection("products", List.of(
				VectorDBUtils.fieldType("id", DataType.Int64,
						builder -> builder.withAutoID(true)
								.withPrimaryKey(true)),
				VectorDBUtils.fieldType("type", DataType.VarChar,
						builder -> builder.withMaxLength(255)),
				VectorDBUtils.fieldType("embedding", DataType.FloatVector,
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

		var imageFactory = ImageFactory.getInstance();
		var imgPathTest = "/data/cv/fashion/1000000151.jpg";
		var modelConfig = new ModelConfig();
		try (var modelZoo = modelConfig.clipModelZoo();
			 var multiModelZoo = modelConfig.multilingualCLIPModelZoo();
			 var model = modelConfig.clipModel(modelZoo, multiModelZoo)) {

			var image = imageFactory.fromFile(Paths.get(imgPathTest));
			float[] embedding = model.extractImageFeatures(image);

			var count = products.insert(Map.of(
					"type", List.of("fashion"),
					"embedding", List.of(embeddingToList(embedding))
			)).await().indefinitely();
			LOG.info("insert count: " + count);

			products.flush()
					.await().indefinitely();

			var searchOutputFields = List.of("id", "type");
			var extraParams = Json.createObjectBuilder()
							.add("nprobe", 10)
							.add("offset", 0)
							.build();
			var search = products.search(5, embedding, "embedding", searchOutputFields, extraParams)
					.await().indefinitely();
			// Because we passed just one embedding, we only get one result, but with a list of 5 top results.
			LOG.info("search: " + search.getRowRecords(0));
			// If we passed two embeddings we could use search.getRowRecords(0) and search.getRowRecords(1)
			products.release()
					.await().indefinitely();
			database.close();
		} catch (Throwable th) {
			LOG.error("Error: " + th.getMessage(), th);
		}
	}
}
