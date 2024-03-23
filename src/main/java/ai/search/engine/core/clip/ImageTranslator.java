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

import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.util.NDImageUtils;
import ai.djl.ndarray.NDList;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslatorContext;

public class ImageTranslator implements NoBatchifyTranslator<Image, float[]> {

    /** {@inheritDoc} */
    @Override
    public float[] processOutput(TranslatorContext ctx, NDList list) {
        var array = list.singletonOrThrow();
        return array.toFloatArray();
    }

    /** {@inheritDoc} */
    @Override
    public NDList processInput(TranslatorContext ctx, Image input) {
        var array = input.toNDArray(ctx.getNDManager(), Image.Flag.COLOR);

		// Resize image to 224x224 (the model has this input size)
        float percent = 224f / Math.min(input.getWidth(), input.getHeight());
        int resizedWidth = Math.round(input.getWidth() * percent);
        int resizedHeight = Math.round(input.getHeight() * percent);

        array = NDImageUtils.resize(array, resizedWidth, resizedHeight,
				Image.Interpolation.BICUBIC);
        array = NDImageUtils.centerCrop(array, 224, 224);
		// Change from Height, Width, Channels to Channels, Height, Width
        array = NDImageUtils.toTensor(array);
        var placeholder = ctx.getNDManager().create("");
		// Placeholder to call method get_image_features:
		// https://huggingface.co/docs/transformers/model_doc/clip#transformers.TFCLIPModel.get_image_features
        placeholder.setName("module_method:get_image_features");
        return new NDList(array.expandDims(0), placeholder);
    }
}
