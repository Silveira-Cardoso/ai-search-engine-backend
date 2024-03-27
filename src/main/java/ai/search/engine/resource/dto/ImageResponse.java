package ai.search.engine.resource.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ImageResponse {
	private final String alt;
	@Getter
	private final String url;
}
