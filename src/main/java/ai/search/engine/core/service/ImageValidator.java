package ai.search.engine.core.service;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.jbosslog.JBossLog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

@JBossLog
@ApplicationScoped
public class ImageValidator {

    public boolean isValid(final InputStream in) {
		try {
			if (in.markSupported()) in.mark(0);
			final String mime = URLConnection.guessContentTypeFromStream(in);
			if (in.markSupported()) in.reset();
			return mime != null;
		} catch (IOException ex) {
			LOG.error("Invalid image format", ex);
		}

		return false;
	}
}
