package ai.search.engine.core.vectordb;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VectorDB {

	private final MilvusServiceClient milvusClient;
	private final String databaseName;
	private final ExecutorService executor;

	public VectorDB(String uri, String token, String databaseName) {
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
	public Uni<VectorDBCollection> createCollectionIfNotExists(final String collectionName, List<FieldType> fieldTypes) {
		return Uni.createFrom().<VectorDBCollection>emitter(emitter -> {
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

			emitter.complete(new VectorDBCollection(databaseName, collectionName, milvusClient));
		})
		.emitOn(executor);
	}

	@RunOnVirtualThread
	private Uni<Long> insert(final String collectionName,
								final Map<String, List<?>> fieldAndValues) {
		return Uni.createFrom().emitter(emiter -> {
			var insertParam = createInsertParam(collectionName, fieldAndValues);
			var listenableFuture = milvusClient.insertAsync(insertParam);
			Futures.addCallback(listenableFuture, new FutureCallback<>() {
				@Override
				public void onSuccess(R<MutationResult> result) {
					// TODO Somente testes, remover depois
					System.out.println("Insert success: " + result);
					emiter.complete(result.getData().getInsertCnt());
				}

				@Override
				public void onFailure(Throwable throwable) {
					emiter.fail(throwable);
				}
			}, executor);
		});
	}

	public Uni<Integer> flush(String collectionName) {
		return Uni.createFrom().<Integer>emitter(emiter -> {
			var resultFlush = milvusClient.flush(FlushParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionNames(List.of(collectionName))
					.build());
			System.out.println("result flush: " + resultFlush);
			if (resultFlush.getException() != null) {
				emiter.fail(resultFlush.getException());
				return;
			}

			emiter.complete(resultFlush.getData().getCollFlushTsCount());
		})
		.emitOn(executor);
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
