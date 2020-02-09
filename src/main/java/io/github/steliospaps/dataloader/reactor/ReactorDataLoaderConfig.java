package io.github.steliospaps.dataloader.reactor;

import java.time.Duration;

import lombok.Builder;
import lombok.Data;

@Data
@Builder()
public class ReactorDataLoaderConfig {
	/**
	 * force a down streeam batch operation even if the batch is not full,
	 * so that no request waits longer than this.
	 */
	@Builder.Default
	private Duration maxStartDelay = Duration.ofMillis(10);
	/**
	 * do not send batches bigger than this
	 */
	@Builder.Default
	private int maxBatchSize=100; 
}
