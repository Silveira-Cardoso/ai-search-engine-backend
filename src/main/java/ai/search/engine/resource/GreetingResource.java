package ai.search.engine.resource;

import ai.search.engine.core.model.FileExtensionEnum;
import ai.search.engine.resource.dto.ImageResponse;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import java.util.List;

@Path("/search")
public class GreetingResource {

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
}
