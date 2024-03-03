package ai.search.engine.core.vectordb;

import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateDatabaseParam;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;

public class VectorDBFactory {

	private VectorDBFactory() {
		throw new IllegalStateException("No VectorDBFactory!");
	}

	@RunOnVirtualThread
	public static Uni<VectorDB> getOrCreate(String uri,
											String token,
											String databaseName) {
		var vectorDb = new VectorDB(uri, token, databaseName);
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
						if (!result.getData().getMsg().equals(RpcStatus.SUCCESS_MSG)) {
							emitter.fail(new IllegalStateException("Failed to create database: " + result.getData().getMsg()));
							return;
						}
					}

					emitter.complete(vectorDb);
				})
				.emitOn(vectorDb.executor);
	}
}
