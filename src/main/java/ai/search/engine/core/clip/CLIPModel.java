/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package ai.search.engine.core.clip;

import ai.djl.inference.Predictor;
import ai.djl.modality.cv.Image;
import lombok.SneakyThrows;

import java.util.List;

/**
 * An example of inference using an CLIP model.
 *
 * <p>See this <a
 * href="https://github.com/deepjavalibrary/djl/blob/master/examples/docs/clip_image_text.md">doc</a>
 * for information about this example.
 */
public class CLIPModel implements AutoCloseable {

    private final Predictor<Image, float[]> imageFeatureExtractor;
    private final Predictor<String, float[]> textFeatureExtractor;

	public CLIPModel(Predictor<Image, float[]> imageFeatureExtractor,
					 Predictor<String, float[]> textFeatureExtractorSupplier) {
		this.imageFeatureExtractor = imageFeatureExtractor;
		this.textFeatureExtractor = textFeatureExtractorSupplier;
	}

	@SneakyThrows
    public float[] extractTextFeatures(String input) {
        return textFeatureExtractor.predict(input);
    }

	@SneakyThrows
    public float[] extractImageFeatures(Image input) {
        return imageFeatureExtractor.predict(input);
    }

	@SneakyThrows
	public List<float[]> batchExtractImageFeatures(List<Image> inputs) {
		return imageFeatureExtractor.batchPredict(inputs);
	}

	@Override
	public void close() {
		imageFeatureExtractor.close();
		textFeatureExtractor.close();
	}
}
