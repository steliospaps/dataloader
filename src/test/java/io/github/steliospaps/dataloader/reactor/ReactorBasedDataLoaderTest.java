package io.github.steliospaps.dataloader.reactor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
	@Captor
	private ArgumentCaptor<List<Integer>> listCaptor;

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
		when(listFunction.apply(any())).thenReturn(Mono.just(List.of("res1", "res2")));

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
					verify(listFunction).apply(List.of(1, 2));
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

	@Test
	void testBatching() throws Exception {
		int batchSize = 50;
		int inputCount = 203;
		int batchCount = 5;
		int lastBatchSize = 3;
		int completedImmediatellyCount = inputCount - lastBatchSize;

		when(listFunction.apply(any())).thenAnswer(invocation -> {
			List<Integer> input = invocation.getArgument(0);
			return Mono.just(input.stream().map(i -> "res" + i).collect(Collectors.toList()));
		});

		var dataloader = new AtomicReference<DataLoader<String, Integer>>();
		var results = IntStream.rangeClosed(1, inputCount)//
				.mapToObj(i -> new AtomicReference<CompletableFuture<String>>())//
				.collect(Collectors.toList());

		// using step verifier to delay time
		StepVerifier.withVirtualTime(() -> {
			dataloader.set(ReactorDataLoader.create(listFunction,
					ReactorDataLoaderConfig.builder()//
						.maxBatchSize(batchSize)//
						.build()));
			for (int i = 0; i < inputCount; i++) {
				results.get(i).set(dataloader.get().apply(i));
			}
			return Flux.empty();
		})//
				.then(() -> {
					verify(listFunction, times(batchCount - 1)).apply(listCaptor.capture());
					for (List<Integer> a : listCaptor.getAllValues()) {
						assertEquals(batchSize, a.size());
					}
					for (int i = 0; i < completedImmediatellyCount; i++) {
						CompletableFuture<String> future = results.get(i).get();
						assertTrue(future.isDone(), "expected furture to be complete index (0-indexed): " + i);
						try {
							assertEquals("res" + i, future.get());
						} catch (InterruptedException | ExecutionException e) {
							throw new RuntimeException(e);
						}
					}
					for (int i = completedImmediatellyCount; i < inputCount; i++) {
						assertFalse(results.get(i).get().isDone(),
								"expected furture to not be complete index (0-indexed): " + i);
					}
				})//
				.thenAwait(Duration.ofMillis(200))//
				.then(() -> {
					verify(listFunction, times(batchCount)).apply(listCaptor.capture());
					assertEquals(lastBatchSize, listCaptor.getValue().size());

					try {
						for (int i = completedImmediatellyCount; i < inputCount; i++) {
							CompletableFuture<String> future = results.get(i).get();
							assertTrue(future.isDone(), "expected furture to be complete index (0-indexed): " + i);
							assertEquals("res" + i, future.get());
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})//
				.verifyComplete();
	}
}
