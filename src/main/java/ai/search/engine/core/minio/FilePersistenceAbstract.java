package ai.search.engine.core.minio;

import ai.search.engine.core.service.ByteArrayService;
import com.google.common.collect.Streams;
import com.google.common.io.ByteStreams;
import io.minio.*;
import io.minio.messages.Item;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@JBossLog
public abstract class FilePersistenceAbstract {
	protected final String minioBucket;
	protected final MinioAsyncClient minioClient;
	private final ByteArrayService byteArrayService;
	private final Boolean publicPolicy;
	private final int batchSize;

	protected FilePersistenceAbstract(String minioBucket, MinioAsyncClient minioClient,
									  ByteArrayService byteArrayService, Boolean publicPolicy, int batchSize) {
		this.minioBucket = minioBucket;
		this.minioClient = minioClient;
		this.byteArrayService = byteArrayService;
		createBucketIfNotExists();
		this.publicPolicy = publicPolicy;
		this.batchSize = batchSize;
	}

	@SneakyThrows
	public void putFile(Map.Entry<String, InputStream> fileNameAndContent) {
		String fileName = fileNameAndContent.getKey();
		byte[] fileContent = ByteStreams.toByteArray(fileNameAndContent.getValue());
		var putArgs = PutObjectArgs.builder()
				.bucket(minioBucket)
				.stream(new ByteArrayInputStream(fileContent), fileContent.length, -1)
				.object(fileName)
				.build();
		var completed = minioClient.putObject(putArgs)
				.thenAccept(objectWriteResponse -> LOG.info("Uploaded file " + fileName));
		completed.exceptionally(e -> {
			LOG.error("Ocorreu um erro ao importar o arquivo " + fileName, e);
			return null;
		});
	}

	@SneakyThrows
	public void deleteFile(Map.Entry<String, InputStream> file) {
		var removeArgs = RemoveObjectArgs.builder()
				.bucket(minioBucket)
				.object(file.getKey())
				.build();
		minioClient.removeObject(removeArgs).exceptionally(e -> {
			LOG.error("Ocorreu um erro ao deletar o arquivo " + file.getKey(), e);
			return null;
		});
	}

	@SneakyThrows
	public Map<String, InputStream> getFiles() {
		var iter = minioClient.listObjects(ListObjectsArgs.builder()
				.bucket(minioBucket)
				.maxKeys(batchSize)
				.recursive(true)
				.build());
		return Streams.stream(iter)
				.limit(batchSize)
				.map(this::getNameAndContentByResult)
				.map(CompletableFuture::join)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	@SneakyThrows
	private void createBucketIfNotExists() {
		LOG.info("Checking if bucket " + minioBucket + " exists...");
		minioClient.bucketExists(BucketExistsArgs.builder().bucket(minioBucket).build())
				.thenApply(exists -> {
					if (!exists) {
						try {
							LOG.info("Creating bucket " + minioBucket + " ...");
							minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioBucket).build())
									.thenAccept(makeBucketResponse -> {
										if (publicPolicy) {
											setPublicPolicy();
										}
									});
						} catch (Exception e) {
							LOG.error("Ocorreu um erro ao criar o bucket " + minioBucket, e);
						}
					}

					return null;
				}).join();
	}

	@SneakyThrows
	private void setPublicPolicy() {
		var policyArgs = SetBucketPolicyArgs.builder()
				.bucket(minioBucket)
				.config("{ \"Version\": \"2012-10-17\", \"Statement\": [ { \"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": [ \"s3:GetObject\" ], \"Resource\": [ \"arn:aws:s3:::" + minioBucket + "/*\" ] } ] }")
				.build();
		minioClient.setBucketPolicy(policyArgs).get();
	}

	@SneakyThrows
	private CompletableFuture<Map.Entry<String, InputStream>> getNameAndContentByResult(Result<Item> result) {
		var item = result.get();
		return minioClient.getObject(GetObjectArgs.builder()
				.bucket(minioBucket)
				.object(item.objectName())
				.build())
				.thenApply(byteArrayService::toByteArray)
				.thenApply(inputStream -> Map.entry(item.objectName(), inputStream));
	}
}
