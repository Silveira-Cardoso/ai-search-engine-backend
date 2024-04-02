package ai.search.engine.core.model;

public enum FileExtensionEnum {
	JPG(".jpg"),
	JPEG(".jpeg"),
	PNG(".png");

	FileExtensionEnum(String fileExtension) {
		this.fileExtension = fileExtension;
	}

	final String fileExtension;

	public static boolean isValidFileExtension(String fileName) {
		var lastIndexOfDot = fileName.lastIndexOf('.');
		var fileExtension = fileName.substring(lastIndexOfDot);
		for (var extension : values()) {
			if (extension.fileExtension.equals(fileExtension)) {
				return true;
			}
		}
		return false;
	}
}
