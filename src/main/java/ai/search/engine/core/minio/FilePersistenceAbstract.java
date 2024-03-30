package ai.search.engine.core.minio;

import com.google.common.io.ByteStreams;
import io.minio.*;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@JBossLog
public abstract class FilePersistenceAbstract {
	protected final String minioBucket;
	protected final MinioAsyncClient minioClient;
	private final Boolean publicPolicy;
	private final int batchSize;

	protected FilePersistenceAbstract(String minioBucket, MinioAsyncClient minioClient, Boolean publicPolicy, int batchSize) {
		this.minioBucket = minioBucket;
		this.minioClient = minioClient;
		createBucketIfNotExists();
		this.publicPolicy = publicPolicy;
		this.batchSize = batchSize;
	}

	@SneakyThrows
	public void putFile(Map.Entry<String, InputStream> fileNameAndContent) {
		String fileName = fileNameAndContent.getKey();
		byte[] fileContent;
		try (var in = fileNameAndContent.getValue()) {
			fileContent = ByteStreams.toByteArray(in);
		}
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
		var list = minioClient.listObjects(ListObjectsArgs.builder()
				.bucket(minioBucket)
				.maxKeys(batchSize)
				.build());
		var files = new HashMap<String, InputStream>();
		for (var item : list) {
			minioClient.getObject(GetObjectArgs.builder()
						.bucket(minioBucket)
						.object(item.get().objectName())
						.build())
					.thenAccept(response -> files.put(response.object(), response))
					.get();
		}

		return files;
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
				});
	}

	@SneakyThrows
	private void setPublicPolicy() {
		var policyArgs = SetBucketPolicyArgs.builder()
				.bucket(minioBucket)
				.config("{ \"Version\": \"2012-10-17\", \"Statement\": [ { \"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": [ \"s3:GetObject\" ], \"Resource\": [ \"arn:aws:s3:::" + minioBucket + "/*\" ] } ] }")
				.build();
		minioClient.setBucketPolicy(policyArgs).get();
	}
}
