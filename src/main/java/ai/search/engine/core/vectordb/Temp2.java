package ai.search.engine.core.vectordb;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.ndarray.NDList;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslatorContext;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import io.quarkus.fs.util.ZipUtils;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Temp2 {

	public static void main(String[] args) throws Exception {
		//https://www.youtube.com/watch?v=SuNpVql6Oec
		var sentences = new String[] { "Esse cachorro é bom", "Esse cachorro não é mau" };

		//var inputs = Map.of("input_ids", OnnxTensor.createTensor(env, inputIds),
		//		 			"attention_mask", OnnxTensor.createTensor(env, attentionMask));
		//session.run(inputs, Set.of("sentence_embedding"));
		//sentence_embedding
		//https://huggingface.co/canavar/clip-ViT-B-32-multilingual-v1-ONNX

		//ZipUtils.unzip(Paths.get("C:\\Users\\Alves\\Desktop\\cnn\\zoo-model.zip"), Paths.get("C:\\Users\\Alves\\Desktop\\cnn\\"));

		var criteria = Criteria.builder()
				.setTypes(String.class, float[].class)
				.optProgress(new ProgressBar("Loading model...", 100))
				.optModelUrls("https://huggingface.co/canavar/clip-ViT-B-32-multilingual-v1-ONNX/resolve/main/zoo-model.zip?download=true")
				.optTranslator(new TextTranslator2())
				.optEngine("OnnxRuntime") // use OnnxRuntime engine by default
				.build();
		try (var model = criteria.loadModel()) {
			var result = model.newPredictor().predict(sentences[0]);
			System.out.println(result.length);
		}
	}

	public static class TextTranslator2 implements NoBatchifyTranslator<String, float[]> {

		private final HuggingFaceTokenizer tokenizer;

		public TextTranslator2() {
			tokenizer = HuggingFaceTokenizer.newInstance("canavar/clip-ViT-B-32-multilingual-v1-ONNX");
		}

		/** {@inheritDoc} */
		@Override
		public float[] processOutput(TranslatorContext ctx, NDList list) {
			return list.getLast().toFloatArray();
		}

		/** {@inheritDoc} */
		@Override
		public NDList processInput(TranslatorContext ctx, String input) {
			final var encoding = tokenizer.encode(input);
			final var attention = ctx.getNDManager().create(encoding.getAttentionMask());
			final var inputIds = ctx.getNDManager().create(encoding.getIds());
			return new NDList(inputIds.expandDims(0), attention.expandDims(0));
		}
	}
}
