package io.github.steliospaps.dataloader.reactor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.github.steliospaps.dataloader.DataLoader;
import reactor.core.publisher.EmitterProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class ReactorDataLoader {

	private static final ReactorDataLoaderConfig DEFAULT_CONFIG = ReactorDataLoaderConfig.builder().build();

	public static <R, T> DataLoader<R, T> create(ListBatchFunction<R, T> listFunction, ReactorDataLoaderConfig config) {
		return new DataLoader<R, T>() {
			private EmitterProcessor<Tuple2<T, Consumer<R>>> emitterProcessor = EmitterProcessor.create();

			{
				emitterProcessor//
						.bufferTimeout(config.getMaxBatchSize(), config.getMaxStartDelay())//
						.map(list -> {
							List<T> input = list.stream().map(Tuple2::getT1).collect(Collectors.toList());
							List<Consumer<R>> outputConsumers = list.stream().map(Tuple2::getT2)
									.collect(Collectors.toList());
							return Tuples.of(input, outputConsumers);
						})//
						.flatMap(t -> listFunction.apply(t.getT1())//
								.map(res -> Tuples.of(res, t.getT2())))
						.doOnNext(t -> {
							for (int i = 0; i < t.getT2().size(); i++) {
								R res = t.getT1().get(i);
								Consumer<R> con = t.getT2().get(i);
								con.accept(res);
							}
						})//
						.subscribe();
			}

			@Override
			public CompletableFuture<R> apply(T input) {
				CompletableFuture<R> res = new CompletableFuture<R>();
				emitterProcessor.onNext(Tuples.of(input,r -> res.complete(r)));
				return res;
			}

		};
	}

	public static DataLoader<String, Integer> create(ListBatchFunction<String, Integer> listFunction) {
		return create(listFunction, DEFAULT_CONFIG);
	}

}
