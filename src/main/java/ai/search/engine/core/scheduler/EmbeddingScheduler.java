package ai.search.engine.core.scheduler;

import ai.djl.ModelException;
import ai.djl.modality.cv.ImageFactory;
import ai.search.engine.core.clip.CLIPModel;
import ai.search.engine.core.config.AppProperties;
import ai.search.engine.core.milvus.VectorDB;
import ai.search.engine.core.minio.AvailableFilePersistence;
import ai.search.engine.core.minio.ImportFilePersistance;
import ai.search.engine.core.service.ImageDatabaseService;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.json.Json;
import lombok.extern.jbosslog.JBossLog;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static ai.search.engine.core.milvus.VectorDBUtils.fieldType;

@JBossLog
@ApplicationScoped
public class EmbeddingScheduler {
	@Inject
	ImportFilePersistance importFilePersistance;
	@Inject
	AvailableFilePersistence availableFilePersistence;
	@Inject
	ImageDatabaseService imageDatabaseService;

	@Scheduled(every = "10s", concurrentExecution=Scheduled.ConcurrentExecution.SKIP)
	public void schedule() {
		LOG.info("Starting image import...");
		var files = importFilePersistance.getFiles();
		LOG.info("Found " + files.size() + " files to process...");
		imageDatabaseService.insertImageBatch(files);
		files.forEach(this::moveFileToAvaiableAndDeleteFromImport);
		LOG.info("Finished image import...");
	}

	private void moveFileToAvaiableAndDeleteFromImport(File file) {
		availableFilePersistence.putFile(file);
		importFilePersistance.deleteFile(file);
	}
}
