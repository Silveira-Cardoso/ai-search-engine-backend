package ai.search.engine.resource;

import ai.search.engine.core.model.FileExtensionEnum;
import ai.search.engine.core.service.ImageDatabaseService;
import ai.search.engine.resource.dto.ImageResponse;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import java.util.List;

@Path("/search")
public class SearchResource {

	private final ImageDatabaseService imageDatabaseService;

	@Inject
	public SearchResource(ImageDatabaseService imageDatabaseService) {
		this.imageDatabaseService = imageDatabaseService;
	}

	@POST
	@Path("/by-image")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
	@RunOnVirtualThread
    public List<ImageResponse> searchByImage(@RestForm FileUpload image) {
		if (!FileExtensionEnum.isValidFileExtension(image.fileName())) {
			return List.of();
		}

		return List.of(new ImageResponse(image.fileName(), image.uploadedFile().toString()));
    }

	@GET
	@Path("/by-predicate")
	@Produces(MediaType.APPLICATION_JSON)
	@RunOnVirtualThread
	public List<ImageResponse> searchByText(@QueryParam("search") String text) {
		var result = imageDatabaseService.searchImages(text);
		return result.stream().map(path -> new ImageResponse(path, path)).toList();
	}
}
