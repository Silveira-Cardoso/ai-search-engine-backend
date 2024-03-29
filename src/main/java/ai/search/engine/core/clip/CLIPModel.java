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

import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.cv.Image;
import ai.djl.ndarray.NDList;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.NoopTranslator;
import ai.djl.util.Pair;
import com.google.common.collect.Streams;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * An example of inference using an CLIP model.
 *
 * <p>See this <a
 * href="https://github.com/deepjavalibrary/djl/blob/master/examples/docs/clip_image_text.md">doc</a>
 * for information about this example.
 */
public class CLIPModel implements AutoCloseable {

    private final ZooModel<NDList, NDList> clip;
    private final Predictor<Image, float[]> imageFeatureExtractor;
    private final Predictor<String, float[]> textFeatureExtractor;
    private final Predictor<Pair<Image, String>, float[]> imgTextComparator;

	public CLIPModel() throws ModelException, IOException {
		this("https://resources.djl.ai/demo/pytorch/clip.zip");
	}

	public CLIPModel(Path modelPath) throws ModelException, IOException {
		this(requireNonNull(modelPath).toUri().toURL().toString());
	}

	public CLIPModel(String uri) throws ModelException, IOException {
		var criteria = Criteria.builder()
				.setTypes(NDList.class, NDList.class)
				.optModelUrls(uri)
				.optTranslator(new NoopTranslator())
				.optEngine("PyTorch")
				.build();
		clip = criteria.loadModel();
		imageFeatureExtractor = clip.newPredictor(new ImageTranslator());
		textFeatureExtractor = clip.newPredictor(new TextTranslator());
		imgTextComparator = clip.newPredictor(new ImageTextTranslator());
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

	@SneakyThrows
    public float[] compareTextAndImage(Image image, String text) {
        return imgTextComparator.predict(new Pair<>(image, text));
    }

	@SneakyThrows
	public List<float[]> batchCompareTextAndImage(List<Image> images, List<String> texts) {
		var batch = Streams.zip(images.stream(), texts.stream(), Pair::new)
				.toList();
		return imgTextComparator.batchPredict(batch);
	}

    /** {@inheritDoc} */
    @Override
    public void close() {
        imageFeatureExtractor.close();
        textFeatureExtractor.close();
        imgTextComparator.close();
        clip.close();
    }
}
