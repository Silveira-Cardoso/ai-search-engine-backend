package ai.search.engine.core.config;

import ai.djl.ndarray.NDList;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.NoopTranslator;
import ai.search.engine.core.clip.CLIPModel;
import ai.search.engine.core.clip.ImageTranslator;
import ai.search.engine.core.clip.MultilingualTextTranslator;
import ai.search.engine.core.clip.TextTranslator;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@JBossLog
@Dependent
public class ModelConfig {

	@Inject
	private AppProperties properties;

	@Produces
	@SneakyThrows
	public CLIPModel clipModel(ZooModel<NDList, NDList> clipModelZoo,
							   ZooModel<String, float[]> multilingualClipModelZoo) {
		if (properties.clipModelMultilingualEnable()) {
			return new CLIPModel(
					clipModelZoo.newPredictor(new ImageTranslator()),
					multilingualClipModelZoo.newPredictor(new MultilingualTextTranslator())
			);
		}

		return new CLIPModel(
				clipModelZoo.newPredictor(new ImageTranslator()),
				clipModelZoo.newPredictor(new TextTranslator())
		);
	}

	@Produces
	@SneakyThrows
	public ZooModel<NDList, NDList> clipModelZoo() {
		return Criteria.builder()
				.setTypes(NDList.class, NDList.class)
				.optModelUrls(properties.clipModelUrl())
				.optTranslator(new NoopTranslator())
				.optEngine("PyTorch")
				.build()
				.loadModel();
	}

	@Produces
	@SneakyThrows
	public ZooModel<String, float[]> multilingualCLIPModelZoo() {
		if (!properties.clipModelMultilingualEnable()) return null;
		return Criteria.builder()
				.setTypes(String.class, float[].class)
				.optModelUrls(properties.clipModelMultilingualUrl())
				.optTranslator(new MultilingualTextTranslator())
				.optEngine("OnnxRuntime") // use OnnxRuntime engine by default
				.build()
				.loadModel();
	}

	public void disposeClipModel(@Disposes CLIPModel clipModel) {
		LOG.debug("Closing clip model");
		clipModel.close();
	}

	public void disposeClipModelZoo(@Disposes ZooModel<NDList, NDList> clipModelZoo) {
		LOG.debug("Closing clip model zoo: " + clipModelZoo.getName());
		clipModelZoo.close();
	}

	public void disposeMultilingualCLIPModelZoo(@Disposes ZooModel<String, float[]> multilingualCLIPModelZoo) {
		if (!properties.clipModelMultilingualEnable()) return;
		LOG.debug("Closing multilingual clip model zoo: " + multilingualCLIPModelZoo.getName());
		multilingualCLIPModelZoo.close();
	}
}
