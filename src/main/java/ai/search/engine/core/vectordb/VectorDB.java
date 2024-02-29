package ai.search.engine.core.vectordb;

import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.model.CLIPModel;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.response.SearchResultsWrapper;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class VectorDB {

    public static void main(String[] args) {
		var databaseName = "aisearch";
		var milvusClient = new MilvusServiceClient(
			ConnectParam.newBuilder()
					.withUri("http://localhost:19530")
					.withToken("root:Milvus")
					.withDatabaseName(databaseName)
					.build()
		);

		var collectionName = "products";
		createDatabaseIfNotExists(milvusClient, databaseName);
		createCollectionIfNotExists(milvusClient, databaseName, collectionName);
		createIndexIfNotExists(milvusClient, databaseName, collectionName, "embedding", "idx_embedding");
		milvusClient.flush(FlushParam.newBuilder()
				.withDatabaseName(databaseName)
				.withCollectionNames(List.of(collectionName))
				.withSyncFlush(true)
				.build());
		milvusClient.close();

		milvusClient = new MilvusServiceClient(
				ConnectParam.newBuilder()
						.withUri("http://localhost:19530")
						.withToken("root:Milvus")
						.withDatabaseName(databaseName)
						.build()
		);

		var imageFactory = ImageFactory.getInstance();
		var imgPathTest = "D:/workspace/vector-database-algorithm/data/cv/fashion/1000000151.jpg";
		try (var model = new CLIPModel()) {

			var image = imageFactory.fromFile(Paths.get(imgPathTest));
			float[] embedding = model.extractImageFeatures(image);

			var fields = new ArrayList<InsertParam.Field>();
			fields.add(new InsertParam.Field("id", List.of(1)));
			fields.add(new InsertParam.Field("type", List.of("fashion")));
			fields.add(new InsertParam.Field("embedding", List.of(toList(embedding))));

			final var insertParam = InsertParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionName(collectionName)
					.withFields(fields)
					.build();

			var resultInsert = milvusClient.insert(insertParam);
			System.out.println("resultInsert: " + resultInsert);
			var resultFlush = milvusClient.flush(FlushParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionNames(List.of(collectionName))
					.withSyncFlush(true)
					.build());
			System.out.println("resultFlush: " + resultFlush);

			var resultLoad = milvusClient.loadCollection(
					LoadCollectionParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionName(collectionName)
							.withSyncLoad(true)
							.build()
			);
			System.out.println("resultLoad: " + resultLoad);

			var searchOutputFields = List.of("id", "type");
			List<List<Float>> searchVectors = List.of(toList(embedding));

			final Integer SEARCH_K = 5;                       // TopK
			final String SEARCH_PARAM = "{\"nprobe\":10, \"offset\":0}";    // Params

			var searchParam = SearchParam.newBuilder()
					.withCollectionName(collectionName)
					.withConsistencyLevel(ConsistencyLevelEnum.STRONG)
					.withMetricType(MetricType.COSINE)
					.withOutFields(searchOutputFields)
					.withTopK(SEARCH_K)
					.withVectors(searchVectors)
					.withVectorFieldName("embedding")
					.withParams(SEARCH_PARAM)
					.build();
			var respSearch = milvusClient.search(searchParam);
			System.out.println("respSearch: " + respSearch);
			var wrapperSearch = new SearchResultsWrapper(respSearch.getData().getResults());
			System.out.println(wrapperSearch.getIDScore(0));
			System.out.println(wrapperSearch.getFieldData("id", 0));

			milvusClient.releaseCollection(
					ReleaseCollectionParam.newBuilder()
							.withCollectionName(collectionName)
							.build());

		} catch (Throwable th) {
			th.printStackTrace();
			System.out.println("Throwable: " + th.getMessage());
		} finally {
			milvusClient.close();
		}
    }

	private static void createIndexIfNotExists(MilvusServiceClient milvusClient,
											   String databaseName,
											   String collectionName,
											   String fieldName,
											   String index) {
		var resultDesc = milvusClient.describeIndex(DescribeIndexParam.newBuilder()
						.withDatabaseName(databaseName)
						.withCollectionName(collectionName)
						.withIndexName(index)
						.build());

		if (resultDesc.getData() != null) return;
		var indexType = IndexType.IVF_FLAT;   // IndexType
		var indexParam = "{\"nlist\":1024}";     // ExtraParam
		var resultIndex = milvusClient.createIndex(CreateIndexParam.newBuilder()
						.withDatabaseName(databaseName)
						.withFieldName(fieldName)
						.withCollectionName(collectionName)
						.withIndexName(index)
						.withIndexType(indexType)
						.withMetricType(MetricType.COSINE)
						.withExtraParam(indexParam)
						.withSyncMode(true)
						.build());
		if (!resultIndex.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			throw new IllegalStateException("Failed to create database: " + resultIndex.getData().getMsg());
		}
	}

	private static List<Float> toList(float[] array) {
		var list = new ArrayList<Float>();
        for (float v : array) {
            list.add(v);
        }
		return list;
	}

	private static void createDatabaseIfNotExists(final MilvusServiceClient milvusClient,
												  final String databaseName) {
		boolean hasDatabase = milvusClient.listDatabases()
				.getData()
				.getDbNamesList()
				.stream()
				.anyMatch(name -> name.equalsIgnoreCase(databaseName));
		if (hasDatabase) return;

		var result = milvusClient.createDatabase(CreateDatabaseParam.newBuilder()
						.withDatabaseName(databaseName)
						.build());
		if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			throw new IllegalStateException("Failed to create database: " + result.getData().getMsg());
		}
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
				.withFieldTypes(List.of(
						newFieldType("id", DataType.Int64,
								builder -> builder.withAutoID(true)
										.withPrimaryKey(true)),
						newFieldType("type", DataType.VarChar,
								builder -> builder.withMaxLength(255)),
						newFieldType("embedding", DataType.FloatVector,
								builder -> builder.withDimension(512))))
				.build());

		if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			throw new IllegalStateException("Failed to create collection: " + result.getData().getMsg());
		}
	}

	private static FieldType newFieldType(final String name,
										  final DataType type) {
		return newFieldType(name, type, Function.identity());
	}

	private static FieldType newFieldType(final String name,
										  final DataType type,
										  final Function<FieldType.Builder, FieldType.Builder> function) {
		return function.apply(FieldType.newBuilder())
				.withName(name)
				.withDataType(type)
				.build();
	}
}
