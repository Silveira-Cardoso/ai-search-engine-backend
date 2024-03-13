package ai.search.engine.core.config;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Setter
@Getter
@ApplicationScoped
public class AppConfig {

	@ConfigProperty(name = "images.path")
	private String imagesPath;

	@ConfigProperty(name = "vector.db.url")
	private String dbUrl;

	@ConfigProperty(name = "vector.db.token")
	private String dbToken;

	@ConfigProperty(name = "vector.db.name")
	private String dbName;
}
