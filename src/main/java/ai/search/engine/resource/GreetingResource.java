package ai.search.engine.resource;

import ai.search.engine.core.model.FileExtensionEnum;
import ai.search.engine.core.service.ImageDatabaseService;
import ai.search.engine.resource.dto.ImageResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import java.util.List;

@Path("/search")
public class GreetingResource {

	@Inject
	private ImageDatabaseService imageDatabaseService;

    @POST
	@Path("/by-image")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public List<ImageResponse> searchByImage(@RestForm FileUpload image) {
		if (!FileExtensionEnum.isValidFileExtension(image.fileName())) {
			return List.of();
		}
		return List.of(new ImageResponse(image.fileName(), "teste"));
    }

	@GET
	@Path("/by-predicate")
	@Produces(MediaType.APPLICATION_JSON)
	public List<ImageResponse> searchByText(@QueryParam("search") String text) {
		var result = imageDatabaseService.searchImages(text);
		return result.stream().map(path -> new ImageResponse(path, path)).toList();
	}
}
