package ai.search.engine.core.scheduler;

import ai.search.engine.core.minio.PublicFilePersistence;
import ai.search.engine.core.minio.ImportFilePersistance;
import ai.search.engine.core.service.ImageDatabaseService;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.jbosslog.JBossLog;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@JBossLog
@ApplicationScoped
public class EmbeddingScheduler {
	@Inject
	ImportFilePersistance importFilePersistance;
	@Inject
	PublicFilePersistence publicFilePersistence;
	@Inject
	ImageDatabaseService imageDatabaseService;

	@Scheduled(every = "10s", concurrentExecution=Scheduled.ConcurrentExecution.SKIP, delay = 10, delayUnit = TimeUnit.SECONDS)
	public void schedule() {
		LOG.info("Starting image import...");
		var files = importFilePersistance.getFiles();
		LOG.info("Found " + files.size() + " files to process...");
		if (files.isEmpty()) {
			LOG.info("Not files where finded to process...");
			return;
		}
		imageDatabaseService.insertImageBatch(files);
		files.entrySet().forEach(this::moveFileToAvaiableAndDeleteFromImport);
		LOG.info("Finished image import...");
	}

	private void moveFileToAvaiableAndDeleteFromImport(Map.Entry<String, InputStream> file) {
		publicFilePersistence.putFile(file);
		importFilePersistance.deleteFile(file);
	}
}
