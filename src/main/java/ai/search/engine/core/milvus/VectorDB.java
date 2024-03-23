package ai.search.engine.core.milvus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.smallrye.mutiny.Uni;
import lombok.extern.jbosslog.JBossLog;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.search.engine.core.milvus.VectorDBUtils.emitException;
import static java.util.Objects.requireNonNull;

@JBossLog
@ThreadSafe
public class VectorDB {

	private static final int MIN_NUM_THREADS = 4;
	private final MilvusServiceClient milvusClient;
	private final String databaseName;
	private final ExecutorService blockingExecutor;
	private final ExecutorService nonBlockingExecutor;

	VectorDB(String uri,
			 String token,
			 String databaseName,
			 ExecutorService blockingExecutor,
			 ExecutorService nonBlockingExecutor) {
		this.databaseName = requireNonNull(databaseName);
        this.milvusClient = new MilvusServiceClient(
				ConnectParam.newBuilder()
						.withUri(uri)
						.withToken(token)
						.withDatabaseName(databaseName)
						.build()
		);
		this.blockingExecutor = requireNonNull(blockingExecutor);
		this.nonBlockingExecutor = requireNonNull(nonBlockingExecutor);
    }

	public Uni<VectorDBCollection> getOrCreateCollection(String collectionName) {
		return getOrCreateCollection(collectionName, List.of());
	}

	public Uni<VectorDBCollection> getOrCreateCollection(String collectionName, List<FieldType> fieldTypes) {
		return VectorDBUtils.<VectorDBCollection>createEmitter(emitter -> {
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

				if (emitException(emitter, result.getException(),
						result.getData(), "Failed to create collection")) return;
			}

			var collection = new VectorDBCollection(databaseName, collectionName, milvusClient,
					blockingExecutor, nonBlockingExecutor);
			emitter.complete(collection);
		})
		.emitOn(blockingExecutor);
	}

	public void close() throws InterruptedException {
		close(1, TimeUnit.MINUTES);
	}

	public void close(long time, TimeUnit timeUnit) throws InterruptedException {
		milvusClient.close();
		blockingExecutor.shutdown();
		nonBlockingExecutor.shutdown();
		if (!blockingExecutor.awaitTermination(time, timeUnit)) {
			LOG.info("blockingExecutor didn't shutdown executor on time %s %s".formatted(time, timeUnit));
		}

		if (!nonBlockingExecutor.awaitTermination(time, timeUnit)) {
			LOG.info("nonBlockingExecutor didn't shutdown executor on time %s %s".formatted(time, timeUnit));
		}
	}

	public static Uni<VectorDB> getOrCreateDatabase(String uri,
													String token,
													String databaseName,
													ExecutorService blockingExecutor,
													ExecutorService nonBlockingExecutor) {
		var vectorDb = new VectorDB(uri, token, databaseName, blockingExecutor, nonBlockingExecutor);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			blockingExecutor.shutdown();
			nonBlockingExecutor.shutdown();
		}));

		return VectorDBUtils.<VectorDB>createEmitter(emitter -> {
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
				.emitOn(blockingExecutor);
	}

	public static Uni<VectorDB> getOrCreateDatabase(String uri,
													String token,
													String databaseName) {
		var blocking = Executors.newFixedThreadPool(Math.max(Runtime.getRuntime().availableProcessors(), MIN_NUM_THREADS));
		var nonBlocking = Executors.newVirtualThreadPerTaskExecutor();
		return getOrCreateDatabase(uri, token, databaseName, blocking, nonBlocking);
	}
}
