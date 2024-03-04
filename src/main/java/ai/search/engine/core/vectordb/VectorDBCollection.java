package ai.search.engine.core.vectordb;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.FlushResponse;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
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
import jakarta.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ai.search.engine.core.vectordb.VectorDBUtils.emitException;

public class VectorDBCollection {

	private final String databaseName;
	private final String collectionName;
	private final MilvusServiceClient milvusClient;
	private final ExecutorService blockingExecutor;
	private final ExecutorService nonBlockingExecutor;

	VectorDBCollection(String databaseName,
					   String collectionName,
					   MilvusServiceClient milvusClient) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.milvusClient = milvusClient;
		// Perhaps a shared executor between instance collections?
		// Or a blockingExecutor for each instance and a nonBlockingExecutor shared between all?
		// because the nonBlockingExecutor works just with non blocking code.
		// Benchmark may be the answer
		this.blockingExecutor = Executors.newSingleThreadExecutor();
		this.nonBlockingExecutor = Executors.newSingleThreadExecutor();
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
				.emitOn(blockingExecutor);
	}

	@RunOnVirtualThread
	public Uni<Long> insert(final Map<String, List<?>> fieldAndValues) {
		return Uni.createFrom().emitter(emitter -> {
			var insertParam = createInsertParam(collectionName, fieldAndValues);
			var listenableFuture = milvusClient.insertAsync(insertParam);
			EmitterToFutureCallBack.emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> result.getData().getInsertCnt());
		});
	}

	@RunOnVirtualThread
	public Uni<FlushResponse> flush() {
		return Uni.createFrom().<FlushResponse>emitter(emitter -> {
					var resultFlush = milvusClient.flush(FlushParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionNames(List.of(collectionName))
							.build());
					if (emitException(emitter, resultFlush.getException())) return;
					emitter.complete(resultFlush.getData());
				})
				.emitOn(blockingExecutor);
	}

	@RunOnVirtualThread
	public Uni<Void> load() {
		return Uni.createFrom().emitter(emitter -> {
			var resultLoad = milvusClient.loadCollection(
					LoadCollectionParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionName(collectionName)
							.build()
			);

			if (emitException(emitter, resultLoad.getException(),
					resultLoad.getData(), "Failed to load index")) return;
			emitter.complete(null);
		})
		.emitOn(blockingExecutor)
		.replaceWithVoid();
	}

	@RunOnVirtualThread
	public Uni<Void> release() {
		return Uni.createFrom().emitter(emitter -> {
			var resultRelease = milvusClient.releaseCollection(
					ReleaseCollectionParam.newBuilder()
							.withCollectionName(collectionName)
							.build());

			if(emitException(emitter, resultRelease.getException(),
					resultRelease.getData(), "Failed to release collection")) return;
			emitter.complete(null);
		})
		.emitOn(blockingExecutor)
		.replaceWithVoid();
	}

	public Uni<SearchResultsWrapper> search(int searchK,
											float[] embedding,
											String embeddingFieldName,
											List<String> outFields,
											JsonObject extraSearchParam) {
		return search(searchK, List.of(embedding), embeddingFieldName, outFields, extraSearchParam);
	}

	@RunOnVirtualThread
	public Uni<SearchResultsWrapper> search(int searchK,
											List<float[]> embeddings,
											String embeddingFieldName,
											List<String> outFields,
											JsonObject extraSearchParam) {
		return Uni.createFrom().emitter(emitter -> {
			var searchVectors = embeddings.stream()
					.map(VectorDBUtils::embeddingToList)
					.toList();
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

			var listenableFuture = milvusClient.searchAsync(searchParam);
			EmitterToFutureCallBack.emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> new SearchResultsWrapper(result.getData().getResults()));
		});
	}

	/**
	 * This method is used by the VectorDB to
	 * create a hook so when the VectorDB is closed,
	 * it shuts down the executors.
	 * It's package private so that the user
	 * don't manage by itself the executors,
	 * breaking the encapsulation.
	 */
	void close() {
		if (!blockingExecutor.isShutdown()) blockingExecutor.shutdown();
		if (!nonBlockingExecutor.isShutdown()) nonBlockingExecutor.shutdown();
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
