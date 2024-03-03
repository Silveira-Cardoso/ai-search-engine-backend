package ai.search.engine.core.vectordb;

import io.milvus.grpc.DataType;
import io.milvus.param.collection.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class VectorDBUtils {

	private VectorDBUtils() {
		throw new IllegalArgumentException("No DBUtils!");
	}

	public static FieldType newFieldType(final String name,
										  final DataType type) {
		return newFieldType(name, type, Function.identity());
	}

	public static FieldType newFieldType(final String name,
										  final DataType type,
										  final Function<FieldType.Builder, FieldType.Builder> function) {
		return function.apply(FieldType.newBuilder())
				.withName(name)
				.withDataType(type)
				.build();
	}

	public static List<Float> embeddingToList(float[] array) {
		var list = new ArrayList<Float>();
		for (float v : array) {
			list.add(v);
		}
		return list;
	}
}
