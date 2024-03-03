package ai.search.engine.core.vectordb;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.response.SearchResultsWrapper;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.json.Json;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ai.search.engine.core.vectordb.VectorDBUtils.embeddingToList;

public class VectorDBCollection {

	private final String databaseName;
	private final String collectionName;
	private final MilvusServiceClient milvusClient;
	private final ExecutorService executor;

	VectorDBCollection(String databaseName,
					   String collectionName,
					   MilvusServiceClient milvusClient) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.milvusClient = milvusClient;
		this.executor = Executors.newSingleThreadExecutor();
    }

	@RunOnVirtualThread
	public Uni<Boolean> createIndexIfNotExists(String fieldName,
											   String indexName,
											   Json indexParam,
											   IndexType indexType,
											   MetricType metricType) {
		return Uni.createFrom().<Boolean>emitter(emiter -> {
					var resultDesc = milvusClient.describeIndex(DescribeIndexParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionName(collectionName)
							.withIndexName(indexName)
							.build());

					if (null == resultDesc.getData()) {
						var resultIndex = milvusClient.createIndex(CreateIndexParam.newBuilder()
								.withDatabaseName(databaseName)
								.withFieldName(fieldName)
								.withCollectionName(collectionName)
								.withIndexName(indexName)
								.withIndexType(indexType)
								.withMetricType(metricType)
								.withExtraParam(indexParam.toString())
								.withSyncMode(true)
								.build());
						if (!resultIndex.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
							emiter.fail(new IllegalStateException("Failed to create index: " + resultIndex.getData().getMsg()));
						}
					}

					emiter.complete(true);
				})
				.emitOn(executor);
	}

	private void a(float[] embedding, List<String> outFields) {
		var resultLoad = milvusClient.loadCollection(
				LoadCollectionParam.newBuilder()
						.withDatabaseName(databaseName)
						.withCollectionName(collectionName)
						.withSyncLoad(true)
						.build()
		);
		System.out.println("resultLoad: " + resultLoad);

		List<List<Float>> searchVectors = List.of(embeddingToList(embedding));

		final Integer SEARCH_K = 5;                       // TopK
		final String SEARCH_PARAM = "{\"nprobe\":10, \"offset\":0}";    // Params

		var searchParam = SearchParam.newBuilder()
				.withCollectionName(collectionName)
				.withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
				.withMetricType(MetricType.COSINE)
				.withOutFields(outFields)
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
	}
}
