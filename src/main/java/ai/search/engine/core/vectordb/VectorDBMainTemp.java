package ai.search.engine.core.vectordb;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.model.CLIPModel;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import jakarta.json.Json;
import lombok.extern.jbosslog.JBossLog;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static ai.search.engine.core.vectordb.VectorDBUtils.embeddingToList;
import static ai.search.engine.core.vectordb.VectorDBUtils.newFieldType;

@JBossLog
public class VectorDBMainTemp {

	public static void main(String[] args) {
		LOG.info("Starting vector search...");
		var databaseName = "aisearch";
		VectorDB db = VectorDB.getOrCreate("http://localhost:19530", "root:Milvus", databaseName)
				.await().indefinitely();
		VectorDBCollection products = db.getOrCreate("products", List.of(
				newFieldType("id", DataType.Int64,
						builder -> builder.withAutoID(true)
								.withPrimaryKey(true)),
				newFieldType("type", DataType.VarChar,
						builder -> builder.withMaxLength(255)),
				newFieldType("embedding", DataType.FloatVector,
						builder -> builder.withDimension(512))))
				.await().indefinitely();

		var indexParam = Json.createObjectBuilder()
				.add("nlist", 1024)
				.build();
		products.createIndexIfNotExists("embedding", "idx_embedding", indexParam, IndexType.IVF_FLAT, MetricType.COSINE)
				.await().indefinitely();
		products.load()
				.await().indefinitely();

		var imageFactory = ImageFactory.getInstance();
		var imgPathTest = "D:/workspace/vector-database-algorithm/data/cv/fashion/1000000151.jpg";
		try (var model = new CLIPModel()) {

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
			db.close();
		} catch (Throwable th) {
			LOG.error("Error: " + th.getMessage(), th);
		}
	}
}
