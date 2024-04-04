package ai.search.engine.core.scheduler;

import ai.search.engine.core.minio.ImportFilePersistence;
import ai.search.engine.core.minio.PublicFilePersistence;
import ai.search.engine.core.service.ImageDatabaseService;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@JBossLog
@ApplicationScoped
public class EmbeddingScheduler {

	private final ImportFilePersistence importFilePersistence;
	private final PublicFilePersistence publicFilePersistence;
	private final ImageDatabaseService imageDatabaseService;

	@Inject
	public EmbeddingScheduler(ImportFilePersistence importFilePersistence,
							  PublicFilePersistence publicFilePersistence,
							  ImageDatabaseService imageDatabaseService) {
		this.importFilePersistence = importFilePersistence;
		this.publicFilePersistence = publicFilePersistence;
		this.imageDatabaseService = imageDatabaseService;
	}

	@Scheduled(every = "5s",
			   concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
			   delay = 10,
			   delayUnit = TimeUnit.SECONDS)
	public void schedule() {
		LOG.info("Starting image import...");
		var files = importFilePersistence.getFiles();
		LOG.info("Found " + files.size() + " files to process...");
		if (files.isEmpty()) {
			LOG.info("Not files where found to process...");
			return;
		}
		try {
			imageDatabaseService.insertImageBatch(files);
			files.entrySet()
					.forEach(this::moveFileToAvailableAndDeleteFromImport);
		} finally {
			files.values()
					.forEach(this::safeClose);
		}
		LOG.info("Finished image import...");
	}

	@SneakyThrows
	private void moveFileToAvailableAndDeleteFromImport(Map.Entry<String, InputStream> file) {
		publicFilePersistence.putFile(file);
		importFilePersistence.deleteFile(file);
	}

	private void safeClose(InputStream inputStream) {
		try {
			inputStream.close();
		} catch (Exception e) {
			LOG.error("Failed to close input stream", e);
		}
	}
}
