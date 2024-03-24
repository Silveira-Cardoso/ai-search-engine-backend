package ai.search.engine.core.vectordb;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.ndarray.NDList;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslatorContext;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;

import java.util.Map;
import java.util.Set;

public class Temp {

	public static void main(String[] args) throws Exception {
		var imageFactory = ImageFactory.getInstance();
		var imgPathTest = "/data/cv/fashion/1000000151.jpg";
		var onnx = "D:/workspace/clip-ViT-B-32-multilingual-v1-ONNX/clip-ViT-B-32-multilingual-v1-ONNX.zip";

		//https://www.youtube.com/watch?v=SuNpVql6Oec
		var sentences = new String[] {"Esse cachorro é bom"}; //, "Esse cachorro não é mau"};

		var env = OrtEnvironment.getEnvironment();
		var session = env.createSession("D:\\workspace\\models\\clip-ViT-B-32-multilingual-v1-ONNX\\model.onnx");
		var tokenizer = HuggingFaceTokenizer.newInstance("canavar/clip-ViT-B-32-multilingual-v1-ONNX");
		var encodings = tokenizer.batchEncode(sentences);

		var inputIds = new long[encodings.length][];
		var attentionMask = new long[encodings.length][];

		for (int i = 0; i < encodings.length; ++i) {
			inputIds[i] = encodings[i].getIds();
			attentionMask[i] = encodings[i].getAttentionMask();
		}

		var inputs = Map.of("input_ids", OnnxTensor.createTensor(env, inputIds),
				 			"attention_mask", OnnxTensor.createTensor(env, attentionMask));

		session.run(inputs, Set.of("sentence_embedding"));
		//sentence_embedding
		//https://huggingface.co/canavar/clip-ViT-B-32-multilingual-v1-ONNX
		try (var outputs = session.run(inputs, Set.of("sentence_embedding"))) {
			outputs.get(0).getValue();
		}

		//var clip = new CLIPMultilingualTextEmbedding(Paths.get(onnx));
		//System.out.println(clip.extractTextFeatures("guilherme"));
	}

	public static class TextTranslator2 implements NoBatchifyTranslator<String, float[]> {

		private final HuggingFaceTokenizer tokenizer;

		public TextTranslator2() {
			tokenizer = HuggingFaceTokenizer.newInstance("canavar/clip-ViT-B-32-multilingual-v1-ONNX");
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
			//final var placeholder = ctx.getNDManager().create("");
			// Placeholder to call method get_text_features:
			// https://huggingface.co/docs/transformers/model_doc/clip#transformers.TFCLIPModel.get_text_features
			//placeholder.setName("module_method:get_text_features");
			// (input_ids, attention_mask)
			return new NDList(inputIds.expandDims(0), attention.expandDims(0)); //, placeholder);
		}
	}
}
