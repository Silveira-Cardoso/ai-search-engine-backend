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
package ai.search.engine.core.vectordb;

import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDList;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.NoopTranslator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An example of inference using an CLIP model.
 *
 * <p>See this <a
 * href="https://github.com/deepjavalibrary/djl/blob/master/examples/docs/clip_image_text.md">doc</a>
 * for information about this example.
 */
public class CLIPMultilingualTextEmbedding implements AutoCloseable {

    private final ZooModel<NDList, NDList> clip;
    private final Predictor<String, float[]> textFeatureExtractor;

	public CLIPMultilingualTextEmbedding(Path modelPath) throws ModelException, IOException {
		var criteria = Criteria.builder()
				.setTypes(NDList.class, NDList.class)
				.optModelPath(modelPath)
				.optTranslator(new NoopTranslator())
				.optEngine("PyTorch")
				.build();
		clip = criteria.loadModel();
		textFeatureExtractor = clip.newPredictor(new Temp.TextTranslator2());
	}

	@SneakyThrows
    public float[] extractTextFeatures(String input) {
        return textFeatureExtractor.predict(input);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        textFeatureExtractor.close();
        clip.close();
    }
}
