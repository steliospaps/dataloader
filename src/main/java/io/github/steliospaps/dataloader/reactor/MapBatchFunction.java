package io.github.steliospaps.dataloader.reactor;

import java.util.List;
import java.util.Map;

import reactor.core.publisher.Mono;

/**
 * allows a sparse result from the batching function
 * @author stelios
 *
 * @param <R>
 * @param <T>
 */
public interface MapBatchFunction<R,T> {
	/**
	 * 
	 * @param input a list of inputs
	 * @return Mono yielding a Map with keys being the input. 
	 */
	Mono<Map<T,R>> apply(List<T> input);
}
