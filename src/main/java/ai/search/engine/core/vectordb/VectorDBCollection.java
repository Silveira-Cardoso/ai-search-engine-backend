package ai.search.engine.core.vectordb;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.response.SearchResultsWrapper;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import jakarta.json.JsonObject;

import java.util.List;
import java.util.Map;
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
											   JsonObject indexParam,
											   IndexType indexType,
											   MetricType metricType) {
		return Uni.createFrom().<Boolean>emitter(emitter -> {
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
						if (emitException(emitter, resultIndex.getException(),
								resultIndex.getData(), "Failed to create index")) return;
					}

					emitter.complete(true);
				})
				.emitOn(executor);
	}

	@RunOnVirtualThread
	public Uni<Long> insert(final Map<String, List<?>> fieldAndValues) {
		return Uni.createFrom().emitter(emitter -> {
			var insertParam = createInsertParam(collectionName, fieldAndValues);
			var listenableFuture = milvusClient.insertAsync(insertParam);
			EmitterToFutureCallBack.emitterToCallback(emitter, listenableFuture, executor,
					result -> result.getData().getInsertCnt());
		});
	}

	public Uni<Integer> flush() {
		return Uni.createFrom().<Integer>emitter(emitter -> {
					var resultFlush = milvusClient.flush(FlushParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionNames(List.of(collectionName))
							.build());
					if (emitException(emitter, resultFlush.getException())) return;
					emitter.complete(resultFlush.getData().getCollFlushTsCount());
				})
				.emitOn(executor);
	}

	public Uni<SearchResultsWrapper> search(int searchK,
							  float[] embedding,
							  String embeddingFieldName,
							  List<String> outFields,
							  JsonObject extraSearchParam) {
		return Uni.createFrom().emitter(emitter -> {
			var resultLoad = milvusClient.loadCollection(
					LoadCollectionParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionName(collectionName)
							.build()
			);

			if (emitException(emitter, resultLoad.getException(),
					resultLoad.getData(), "Failed to load index")) return;

			boolean failedRelease;
			R<SearchResults> respSearch;
			try {
				var searchVectors = List.of(embeddingToList(embedding));
				var searchParam = SearchParam.newBuilder()
						.withCollectionName(collectionName)
						.withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
						.withMetricType(MetricType.COSINE)
						.withOutFields(outFields)
						.withTopK(searchK)
						.withVectors(searchVectors)
						.withVectorFieldName(embeddingFieldName)
						.withParams(extraSearchParam.toString())
						.build();

				respSearch = milvusClient.search(searchParam);
			} finally {
				var resultRelease = milvusClient.releaseCollection(
						ReleaseCollectionParam.newBuilder()
								.withCollectionName(collectionName)
								.build());

				failedRelease = emitException(emitter, resultRelease.getException(),
						resultRelease.getData(), "Failed to release collection");
            }

			if (failedRelease) return;
			emitter.complete(new SearchResultsWrapper(respSearch.getData().getResults()));
		});
	}

	private static boolean emitException(UniEmitter<?> emitter, Exception apiException) {
		if (apiException != null) {
			emitter.fail(apiException);
			return true;
		}

		return false;
	}

	private static boolean emitException(UniEmitter<?> emitter, Exception apiException,
										 RpcStatus status, String statusFailedMsg) {
		if (!status.getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			emitter.fail(new IllegalStateException(statusFailedMsg));
			return true;
		}

		return emitException(emitter, apiException);
	}

	private InsertParam createInsertParam(String collectionName,
										  Map<String, List<?>> fieldAndValues) {
		var fields = fieldAndValues.entrySet()
				.stream()
				.map(entry -> new InsertParam.Field(entry.getKey(), entry.getValue()))
				.toList();

		return InsertParam.newBuilder()
				.withDatabaseName(databaseName)
				.withCollectionName(collectionName)
				.withFields(fields)
				.build();
	}
}
