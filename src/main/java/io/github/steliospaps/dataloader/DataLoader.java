package io.github.steliospaps.dataloader;

import java.util.concurrent.CompletableFuture;

public interface DataLoader<R, T> {

	CompletableFuture<R> apply(T input);
	
}
