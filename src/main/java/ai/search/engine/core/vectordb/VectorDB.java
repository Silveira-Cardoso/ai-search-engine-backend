package ai.search.engine.core.vectordb;

import ai.djl.ModelException;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.translate.TranslateException;
import ai.search.engine.core.model.CLIPModel;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.dml.InsertParam;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class VectorDB {

    public static void main(String[] args) throws ModelException, TranslateException, IOException {
		final MilvusServiceClient milvusClient = new MilvusServiceClient(
				ConnectParam.newBuilder()
						.withUri("http://localhost:19530")
						.withToken("root:Milvus")
						.build()
		);

		var databaseName = "aisearch";
		var collectionName = "products";
		createCollectionIfNotExists(milvusClient, databaseName, collectionName);

		var imageFactory = ImageFactory.getInstance();
		var imgPathTest = "D:/workspace/vector-database-algorithm/data/cv/fashion/1000000151.jpg";
		try (var model = new CLIPModel()) {

			var image = imageFactory.fromFile(Paths.get(imgPathTest));
			float[] embedding = model.extractImageFeatures(image);

			var fields = new ArrayList<InsertParam.Field>();
			fields.add(new InsertParam.Field("id", List.of(1)));
			fields.add(new InsertParam.Field("type", List.of("fashion")));
			fields.add(new InsertParam.Field("embedding", toList(embedding)));

			final var insertParam = InsertParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionName(collectionName)
					.withFields(fields)
					.build();

			milvusClient.insert(insertParam);
			milvusClient.flush(FlushParam.newBuilder()
					.build());
		} catch (Throwable th) {
			System.out.println(th.getMessage());
		} finally {
			milvusClient.close();
		}
    }

	private static List<Float> toList(float[] array) {
		var list = new ArrayList<Float>();
        for (float v : array) {
            list.add(v);
        }
		return list;
	}

	private static void createCollectionIfNotExists(final MilvusServiceClient milvusClient,
													final String databaseName,
													final String collectionName) {
		var hasCollection = milvusClient.hasCollection(HasCollectionParam.newBuilder()
				.withDatabaseName(databaseName)
				.withCollectionName(collectionName)
				.build());
		if (hasCollection.getData()) return;

		var result = milvusClient.createCollection(CreateCollectionParam.newBuilder()
				.withDatabaseName(databaseName)
				.withCollectionName(collectionName)
				//.withFieldTypes(List.of(FieldType.newBuilder().build(), FieldType.STRING, FieldType.FLOAT_LIST))
				.build());

		if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			throw new IllegalStateException("Failed to create collection: " + result.getData().getMsg());
		}
	}
}
