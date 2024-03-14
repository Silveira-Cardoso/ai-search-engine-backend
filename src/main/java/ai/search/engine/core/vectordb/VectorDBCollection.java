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
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.json.JsonObject;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static ai.search.engine.core.vectordb.EmitterToFutureCallBack.emitterToCallback;
import static ai.search.engine.core.vectordb.VectorDBUtils.createEmitter;
import static ai.search.engine.core.vectordb.VectorDBUtils.emitException;

@ThreadSafe
public class VectorDBCollection {

	private final String databaseName;
	private final String collectionName;
	private final MilvusServiceClient milvusClient;
	private final ExecutorService blockingExecutor;
	private final ExecutorService nonBlockingExecutor;

	VectorDBCollection(String databaseName,
					   String collectionName,
					   MilvusServiceClient milvusClient,
					   ExecutorService blockingExecutor,
					   ExecutorService nonBlockingExecutor) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.milvusClient = milvusClient;
		this.blockingExecutor = blockingExecutor;
		this.nonBlockingExecutor = nonBlockingExecutor;
    }

	public Uni<Boolean> createIndexIfNotExists(String fieldName,
											   String indexName,
											   JsonObject indexParam,
											   IndexType indexType,
											   MetricType metricType) {
		return VectorDBUtils.<Boolean>createEmitter(emitter -> {
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

	public Uni<Long> insert(final Map<String, List<?>> fieldAndValues) {
		return createEmitter(emitter -> {
			var insertParam = insertParam(collectionName, fieldAndValues);
			var listenableFuture = milvusClient.insertAsync(insertParam);
			emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> result.getData().getInsertCnt());
		});
	}

	public Uni<FlushResponse> flush() {
		return VectorDBUtils.<FlushResponse>createEmitter(emitter -> {
					var resultFlush = milvusClient.flush(FlushParam.newBuilder()
							.withDatabaseName(databaseName)
							.withCollectionNames(List.of(collectionName))
							.build());
					if (emitException(emitter, resultFlush.getException())) return;
					emitter.complete(resultFlush.getData());
				})
				.emitOn(blockingExecutor);
	}

	public Uni<Void> load() {
		return createEmitter(emitter -> {
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

	public Uni<Void> release() {
		return createEmitter(emitter -> {
			var resultRelease = milvusClient.releaseCollection(
					ReleaseCollectionParam.newBuilder()
							.withCollectionName(collectionName)
							.build());

			if (emitException(emitter, resultRelease.getException(),
					resultRelease.getData(), "Failed to release collection")) return;
			emitter.complete(null);
		})
		.emitOn(blockingExecutor)
		.replaceWithVoid();
	}

	/**
	 * Search with Milvus API with embedding vector as input data field.
	 * Example:
	 * 	var searchOutputFields = List.of("id", "type");
	 * 	var extraParams = Json.createObjectBuilder()
	 * 			.add("nprobe", 10)
	 * 			.add("offset", 0)
	 * 			.build();
	 * 	var search = products.search(5, embedding, "embedding", searchOutputFields, extraParams)
	 * 			.await().indefinitely();
	 * LOG.info("search: " + search.getRowRecords(0));
     * Reference:
     * 	<a href="https://milvus.io/docs/search.md">www.milvus.io</a>
     */
	public Uni<SearchResultsWrapper> search(int searchK,
											float[] embedding,
											String embeddingFieldName,
											List<String> outFields,
											JsonObject extraSearchParam) {
		return search(searchK, List.of(embedding), embeddingFieldName, outFields, extraSearchParam);
	}

	/**
	 * Search with Milvus API with embedding vector as input data field.
	 * Reference:
	 * 	<a href="https://milvus.io/docs/search.md">www.milvus.io</a>
	 */
	public Uni<SearchResultsWrapper> search(int searchK,
											List<float[]> embeddings,
											String embeddingFieldName,
											List<String> outFields,
											JsonObject extraSearchParam) {
		return createEmitter(emitter -> {
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
			emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> new SearchResultsWrapper(result.getData().getResults()));
		});
	}

	/**
	 * Query with Milvus API.
	 * expr: "book_id in [2,4,6,8]"
	 * outFields: List.of("book_id", "title", "author") or List.of("count(*)")
	 * Reference:
	 * 	<a href="https://milvus.io/docs/query.md">www.milvus.io</a>
	 */
	public Uni<QueryResultsWrapper> query(String expr,
										  List<String> outFields,
										  long offset,
										  long limit) {
		return createEmitter(emitter -> {
			var queryParam = queryBuilder(expr, outFields)
					.withOffset(offset)
					.withLimit(limit)
					.build();
			var listenableFuture = milvusClient.queryAsync(queryParam);
			emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> new QueryResultsWrapper(result.getData()));
		});
	}

	public Uni<QueryResultsWrapper> query(String expr,
										  List<String> outFields) {
		return createEmitter(emitter -> {
			var queryParam = queryBuilder(expr, outFields).build();
			var listenableFuture = milvusClient.queryAsync(queryParam);
			emitterToCallback(emitter, listenableFuture, nonBlockingExecutor,
					result -> new QueryResultsWrapper(result.getData()));
		});
	}

	private QueryParam.Builder queryBuilder(String expr, List<String> outFields) {
		return QueryParam.newBuilder()
				.withCollectionName(collectionName)
				.withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
				.withExpr(expr)
				.withOutFields(outFields);
	}

	private InsertParam insertParam(String collectionName,
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
