package ai.search.engine.core.clip;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDList;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslatorContext;

public class TextTranslator implements NoBatchifyTranslator<String, float[]> {

    private final HuggingFaceTokenizer tokenizer;

    public TextTranslator() {
        tokenizer = HuggingFaceTokenizer.newInstance("openai/clip-vit-base-patch32");
    }

    /** {@inheritDoc} */
    @Override
    public float[] processOutput(TranslatorContext ctx, NDList list) {
        return list.singletonOrThrow().toFloatArray();
    }

    /** {@inheritDoc} */
    @Override
    public NDList processInput(TranslatorContext ctx, String input) {
        final var encoding = tokenizer.encode(input);
        final var attention = ctx.getNDManager().create(encoding.getAttentionMask());
        final var inputIds = ctx.getNDManager().create(encoding.getIds());
        final var placeholder = ctx.getNDManager().create("");
		// Placeholder to call method get_text_features:
		// https://huggingface.co/docs/transformers/model_doc/clip#transformers.TFCLIPModel.get_text_features
        placeholder.setName("module_method:get_text_features");
		// (input_ids, attention_mask)
        return new NDList(inputIds.expandDims(0), attention.expandDims(0), placeholder);
    }
}
