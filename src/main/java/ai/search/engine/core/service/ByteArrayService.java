package ai.search.engine.core.service;

import com.google.common.io.ByteStreams;
import io.minio.GetObjectResponse;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@ApplicationScoped
public class ByteArrayService {

	@SneakyThrows
	public InputStream toByteArray(GetObjectResponse response) {
		try (response) {
			var array = ByteStreams.toByteArray(response);
			return new ByteArrayInputStream(array, 0, array.length);
		}
	}

	@SneakyThrows
	public InputStream toByteArray(Path filePath) {
		var array = Files.readAllBytes(filePath);
		return new ByteArrayInputStream(array, 0, array.length);
	}
}
