package ai.search.engine.core.vectordb;

import com.google.common.primitives.Floats;
import io.milvus.grpc.DataType;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.FieldType;
import io.smallrye.mutiny.subscription.UniEmitter;

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
		return Floats.asList(array);
	}

	static boolean emitException(UniEmitter<?> emitter, Exception apiException) {
		if (apiException != null) {
			emitter.fail(apiException);
			return true;
		}

		return false;
	}

	static boolean emitException(UniEmitter<?> emitter, Exception apiException,
								 RpcStatus status, String statusFailedMsg) {
		if (!status.getMsg().equals(RpcStatus.SUCCESS_MSG)) {
			emitter.fail(new IllegalStateException(statusFailedMsg));
			return true;
		}

		return emitException(emitter, apiException);
	}
}
