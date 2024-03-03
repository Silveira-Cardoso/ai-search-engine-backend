package ai.search.engine.core.vectordb;

import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VectorDB {

	final MilvusServiceClient milvusClient;
	final String databaseName;
	final ExecutorService executor;

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
    }

	@RunOnVirtualThread
	public Uni<VectorDBCollection> getOrCreate(final String collectionName, List<FieldType> fieldTypes) {
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
}
