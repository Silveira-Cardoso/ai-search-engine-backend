package ai.search.engine.core.clip;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDList;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslatorContext;

public class MultilingualTextTranslator implements NoBatchifyTranslator<String, float[]> {

	private final HuggingFaceTokenizer tokenizer;

	public MultilingualTextTranslator() {
		tokenizer = HuggingFaceTokenizer.newInstance("canavar/clip-ViT-B-32-multilingual-v1-ONNX");
	}

	@Override
	public float[] processOutput(TranslatorContext ctx, NDList list) {
		return list.getLast().toFloatArray();
	}

	@Override
	public NDList processInput(TranslatorContext ctx, String input) {
		final var encoding = tokenizer.encode(input);
		final var attention = ctx.getNDManager().create(encoding.getAttentionMask());
		final var inputIds = ctx.getNDManager().create(encoding.getIds());
		return new NDList(inputIds.expandDims(0), attention.expandDims(0));
	}
}
