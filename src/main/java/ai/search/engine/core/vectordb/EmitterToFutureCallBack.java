package ai.search.engine.core.vectordb;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.param.R;
import io.smallrye.mutiny.subscription.UniEmitter;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

class EmitterToFutureCallBack<IN, OUT> implements FutureCallback<R<IN>> {

	private final UniEmitter<OUT> emitter;
	private final Function<R<IN>, OUT> function;

	EmitterToFutureCallBack(final UniEmitter<OUT> emitter,
							final Function<R<IN>, OUT> function) {
        this.emitter = emitter;
		this.function = function;
    }

    @Override
	public void onSuccess(R<IN> result) {
		emitter.complete(function.apply(result));
	}

	@Override
	public void onFailure(Throwable throwable) {
		emitter.fail(throwable);
	}

	public static <IN, OUT> void emitterToCallback(UniEmitter<OUT> emitter,
												   ListenableFuture<R<IN>> listenableFuture,
												   ExecutorService executor,
												   Function<R<IN>, OUT> function) {
		Futures.addCallback(listenableFuture, new EmitterToFutureCallBack<>(emitter, function), executor);
	}
}
