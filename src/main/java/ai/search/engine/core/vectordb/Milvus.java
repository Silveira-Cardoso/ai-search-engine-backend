package ai.search.engine.core.vectordb;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.MutationResult;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.highlevel.dml.response.InsertResponse;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.json.Json;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Milvus {

	private final MilvusServiceClient milvusClient;
	private final String databaseName;
	private final ExecutorService executor;

	public Milvus(String uri, String token, String databaseName) {
		this.databaseName = Objects.requireNonNull(databaseName);
        this.milvusClient = new MilvusServiceClient(
				ConnectParam.newBuilder()
						.withUri(uri)
						.withToken(token)
						.withDatabaseName(databaseName)
						.build()
		);
		this.executor = Executors.newSingleThreadExecutor();
    }

	@RunOnVirtualThread
	public Uni<Boolean> createDatabaseIfNotExists() {
		return Uni.createFrom().<Boolean>emitter(emitter -> {
			boolean hasDatabase = milvusClient.listDatabases()
					.getData()
					.getDbNamesList()
					.stream()
					.anyMatch(name -> name.equalsIgnoreCase(databaseName));
			if (!hasDatabase) {
				var result = milvusClient.createDatabase(CreateDatabaseParam.newBuilder()
						.withDatabaseName(databaseName)
						.build());
				if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
					emitter.fail(new IllegalStateException("Failed to create database: " + result.getData().getMsg()));
					return;
				}
			}

			emitter.complete(true);
		})
		.emitOn(executor);
	}

	@RunOnVirtualThread
	public Uni<Boolean> createCollectionIfNotExists(final String collectionName, List<FieldType> fieldTypes) {
		return Uni.createFrom().<Boolean>emitter(emitter -> {
			var hasCollection = milvusClient.hasCollection(HasCollectionParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionName(collectionName)
					.build());
			if (!hasCollection.getData()) {
				var result = milvusClient.createCollection(CreateCollectionParam.newBuilder()
						.withDatabaseName(databaseName)
						.withCollectionName(collectionName)
						.withFieldTypes(fieldTypes)
						.build());

				if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
					emitter.fail(new IllegalStateException("Failed to create collection: " + result.getData().getMsg()));
					return;
				}
			}

			emitter.complete(true);
		})
		.emitOn(executor);
	}

	@RunOnVirtualThread
	public Uni<Boolean> createIndexIfNotExists(String collectionName,
											   String fieldName,
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

	@RunOnVirtualThread
	private Uni<Boolean> insert(String collectionName, Map<String, List<?>> fieldAndValues) {
		return Uni.createFrom().emitter(emiter -> {
			var insertParam = createInsertParam(collectionName, fieldAndValues);
			var listenableFuture = milvusClient.insertAsync(insertParam);
			Futures.addCallback(listenableFuture, new FutureCallback<>() {
				@Override
				public void onSuccess(R<MutationResult> result) {
					// TODO Somente testes, remover depois
					System.out.println("Insert success: " + result.getData().getInsertCnt());
					emiter.complete(true);
				}

				@Override
				public void onFailure(Throwable throwable) {
					emiter.fail(throwable);
				}
			}, executor);
		});
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
