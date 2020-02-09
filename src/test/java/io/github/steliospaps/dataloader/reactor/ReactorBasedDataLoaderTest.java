package io.github.steliospaps.dataloader.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.github.steliospaps.dataloader.DataLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ReactorBasedDataLoaderTest {

	@Mock
	private ListBatchFunction<String, Integer> listFunction;

	@BeforeAll
	public static void setupTimeouts() {
		StepVerifier.setDefaultTimeout(Duration.ofMillis(100));
	}

	@Test
	void testSingle() throws Exception {
		when(listFunction.apply(any())).thenReturn(Mono.just(List.of("res")));

		var dataloader = new AtomicReference<DataLoader<String, Integer>>();
		var f1 = new AtomicReference<CompletableFuture<String>>();
		// using step verifier to delay time
		StepVerifier.withVirtualTime(() -> {
			dataloader.set(ReactorDataLoader.create(listFunction));
			f1.set(dataloader.get().apply(1));
			return Flux.empty();
		})//
				.then(() -> Mockito.verifyNoInteractions(listFunction))//
				.thenAwait(Duration.ofMillis(200))//
				.then(() -> {
					verify(listFunction).apply(List.of(1));
					assertTrue(f1.get().isDone());
					try {
						assertEquals("res", f1.get().get());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})//
				.verifyComplete();
	}

	@Test
	void testMultiple() throws Exception {
		when(listFunction.apply(any())).thenReturn(Mono.just(List.of("res1","res2")));

		var dataloader = new AtomicReference<DataLoader<String, Integer>>();
		var f1 = new AtomicReference<CompletableFuture<String>>();
		var f2 = new AtomicReference<CompletableFuture<String>>();
		// using step verifier to delay time
		StepVerifier.withVirtualTime(() -> {
			dataloader.set(ReactorDataLoader.create(listFunction));
			f1.set(dataloader.get().apply(1));
			f2.set(dataloader.get().apply(2));
			return Flux.empty();
		})//
				.then(() -> Mockito.verifyNoInteractions(listFunction))//
				.thenAwait(Duration.ofMillis(200))//
				.then(() -> {
					verify(listFunction).apply(List.of(1,2));
					assertTrue(f1.get().isDone());
					assertTrue(f2.get().isDone());
					try {
						assertEquals("res1", f1.get().get());
						assertEquals("res2", f2.get().get());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})//
				.verifyComplete();
	}
}
