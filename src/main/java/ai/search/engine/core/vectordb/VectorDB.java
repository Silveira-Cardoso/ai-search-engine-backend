package ai.search.engine.core.vectordb;

import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import lombok.extern.jbosslog.JBossLog;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.search.engine.core.vectordb.VectorDBUtils.emitException;

@JBossLog
public class VectorDB {

	private final MilvusServiceClient milvusClient;
	private final String databaseName;
	private final ExecutorService executor;
	private final ConcurrentLinkedQueue<VectorDBCollection> collections;

	VectorDB(String uri, String token, String databaseName) {
		this.databaseName = Objects.requireNonNull(databaseName);
        this.milvusClient = new MilvusServiceClient(
				ConnectParam.newBuilder()
						.withUri(uri)
						.withToken(token)
						.withDatabaseName(databaseName)
						.build()
		);
		this.executor = Executors.newSingleThreadExecutor();
		this.collections = new ConcurrentLinkedQueue<>();
    }

	@RunOnVirtualThread
	public Uni<VectorDBCollection> getOrCreate(String collectionName, List<FieldType> fieldTypes) {
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

			var collection = new VectorDBCollection(databaseName, collectionName, milvusClient);
			collections.add(collection);
			Runtime.getRuntime().addShutdownHook(new Thread(collection::close));
			emitter.complete(collection);
		})
		.emitOn(executor);
	}

	public void close() throws InterruptedException {
		close(1, TimeUnit.MINUTES);
	}

	public void close(long time, TimeUnit timeUnit) throws InterruptedException {
		collections.forEach(VectorDBCollection::close);
		milvusClient.close();
		executor.shutdown();
		if (!executor.awaitTermination(time, timeUnit)) {
			LOG.info("Didn't shutdown executor on time %s %s".formatted(time, timeUnit));
		}
	}

	@RunOnVirtualThread
	public static Uni<VectorDB> getOrCreate(String uri,
											String token,
											String databaseName) {
		var vectorDb = new VectorDB(uri, token, databaseName);
		Runtime.getRuntime().addShutdownHook(new Thread(vectorDb.executor::shutdown));
		return Uni.createFrom().<VectorDB>emitter(emitter -> {
					boolean hasDatabase = vectorDb.milvusClient.listDatabases()
							.getData()
							.getDbNamesList()
							.stream()
							.anyMatch(name -> name.equalsIgnoreCase(databaseName));
					if (!hasDatabase) {
						var result = vectorDb.milvusClient.createDatabase(CreateDatabaseParam.newBuilder()
								.withDatabaseName(databaseName)
								.build());
						if (emitException(emitter, result.getException(),
								result.getData(), "Failed to create database")) return;
					}

					emitter.complete(vectorDb);
				})
				.emitOn(vectorDb.executor);
	}
}
