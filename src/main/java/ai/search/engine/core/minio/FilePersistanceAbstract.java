package ai.search.engine.core.minio;

import com.google.common.io.Files;
import io.minio.*;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@JBossLog
public abstract class FilePersistanceAbstract {
	protected final String minioBucket;
	protected final MinioAsyncClient minioClient;

	protected FilePersistanceAbstract(String minioBucket, MinioAsyncClient minioClient) {
		this.minioBucket = minioBucket;
		this.minioClient = minioClient;
		createBucketIfNotExists();
	}

	@SneakyThrows
	public void putFile(File file) {
		var putArgs = PutObjectArgs.builder()
				.bucket(minioBucket)
				.stream(new FileInputStream(file), file.length(), -1)
				.object(file.getName())
				.build();
		var completed = minioClient.putObject(putArgs).thenAccept(objectWriteResponse -> LOG.info("Uploaded file " + file.getName()));
		completed.exceptionally(e -> {
			LOG.error("Ocorreu um erro ao importar o arquivo " + file.getName(), e);
			return null;
		});
	}

	@SneakyThrows
	public void deleteFile(File file) {
		var removeArgs = RemoveObjectArgs.builder()
				.bucket(minioBucket)
				.object(file.getName())
				.build();
		minioClient.removeObject(removeArgs).exceptionally(e -> {
			LOG.error("Ocorreu um erro ao deletar o arquivo " + file.getName(), e);
			return null;
		});
	}

	@SneakyThrows
	public List<File> getFiles() {
		var list = minioClient.listObjects(ListObjectsArgs.builder()
				.bucket(minioBucket)
				.maxKeys(100)
				.build());
		List<File> files = new ArrayList<>();
		for (var item : list) {
			minioClient.getObject(GetObjectArgs.builder()
					.bucket(minioBucket)
					.object(item.get().objectName())
					.build())
					.thenAccept(response -> {
                        try {
							var file = new File(response.object());
                            Files.write(response.readAllBytes(), file);
							files.add(file);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).get();
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
							minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioBucket).build());
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					return null;
				});
	}
}
