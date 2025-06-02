// pkg/utils/retry.go
package utils

import (
	"math/rand"
	"time"
)

// BackoffConfig holds configuration for calculating backoff durations.
type BackoffConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Jitter          bool
}

// CalculateBackoff computes the next backoff duration based on the retry count
// and the provided backoff configuration.
func CalculateBackoff(retryCount int, config BackoffConfig) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}

	delay := float64(config.InitialInterval)

	for i := 0; i < retryCount; i++ {
		delay *= config.Multiplier
		if delay > float64(config.MaxInterval) {
			delay = float64(config.MaxInterval)
			break
		}
	}

	if config.Jitter {
		// Add jitter: delay ± (delay * 0.25)
		// For rand.Float64() which is [0.0, 1.0), this becomes:
		// jitterFactor := (rand.Float64() * 0.5) - 0.25  // results in [-0.25, 0.25)
		// jitter := delay * jitterFactor
		// delay += jitter
		// Simplified: delay * (1 + jitterFactor) = delay * (0.75 to 1.25)
		// A common way is to add a random percentage of the delay up to a certain cap.
		// Using rand.NewSource for better randomness if called frequently, but time.Now().UnixNano() is okay for less frequent calls.
		// The original implementation: jitter := delay * 0.25 * (2*float64(time.Now().UnixNano()%100)/100 - 1)
		// This is equivalent to: jitter := delay * 0.25 * (random_percentage_between_-1_and_1)
		// Let's use a slightly more standard Go random source for jitter.
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		jitterMultiplier := config.Multiplier * 0.25 // Max jitter is 25% of the current delay
		randomFactor := (r.Float64() * 2) - 1 // random number between -1.0 and 1.0
		jitter := delay * jitterMultiplier * randomFactor
		delay += jitter
	}

	// Ensure delay does not exceed MaxInterval after jitter addition,
	// and is not less than InitialInterval (or some other minimum if desired, though current logic doesn't enforce minimum after jitter).
	if delay > float64(config.MaxInterval) {
		delay = float64(config.MaxInterval)
	}
	if delay < 0 { // Jitter could theoretically make it negative if multiplier is very small, though unlikely with typical values.
		delay = float64(config.InitialInterval) / 2 // Or some small positive value
		if delay == 0 && config.InitialInterval > 0 {
			delay = float64(config.InitialInterval) / 2
		} else if delay == 0 {
			delay = 100 * float64(time.Millisecond) // Absolute minimum if InitialInterval is also 0
		}
	}


	return time.Duration(delay)
}
