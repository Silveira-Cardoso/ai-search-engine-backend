package ai.search.engine.core.config;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Setter
@Getter
@ApplicationScoped
@Accessors(fluent = true)
public class AppProperties {

	@ConfigProperty(name = "images.path.from")
	private String imagesPathFrom;

	@ConfigProperty(name = "images.path.to")
	private String imagesPathTo;

	@ConfigProperty(name = "embeddings.batch.size", defaultValue = "128")
	private int embeddingsBatchSize;

	@ConfigProperty(name = "vector.db.url")
	private String dbUrl;

	@ConfigProperty(name = "vector.db.token")
	private String dbToken;

	@ConfigProperty(name = "vector.db.name")
	private String dbName;
}
