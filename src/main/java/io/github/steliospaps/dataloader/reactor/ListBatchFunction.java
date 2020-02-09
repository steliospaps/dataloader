package io.github.steliospaps.dataloader.reactor;

import java.util.List;
import reactor.core.publisher.Mono;

public interface ListBatchFunction<R,T> {
	/**
	 * 
	 * @param input a list of inputs
	 * @return a list of outputs. output.get(N) is the output corresponding to input.get(N)
	 */
	Mono<List<R>> apply(List<T> input);
}
