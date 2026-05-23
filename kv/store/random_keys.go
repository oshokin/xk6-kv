package store

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// MaxRandomKeysCount bounds randomKeys() output size to avoid unbounded allocations.
const MaxRandomKeysCount int64 = 1_000_000

// validateRandomKeysCount rejects random key counts above MaxRandomKeysCount.
func validateRandomKeysCount(count int64) error {
	if count > MaxRandomKeysCount {
		return fmt.Errorf(
			"%w: randomKeys count must be less than or equal to %d",
			ErrKVOptionsInvalid,
			MaxRandomKeysCount,
		)
	}

	return nil
}

// sampleKeys returns up to count keys, with or without replacement.
func sampleKeys(keys []string, count int64, unique bool) []string {
	if count <= 0 || len(keys) == 0 {
		return []string{}
	}

	if unique {
		return sampleUniqueKeys(keys, count)
	}

	return sampleKeysWithReplacement(keys, count)
}

// sampleUniqueKeys returns up to count distinct keys from keys.
func sampleUniqueKeys(keys []string, count int64) []string {
	if count <= 0 || len(keys) == 0 {
		return []string{}
	}

	total := len(keys)
	if count >= int64(total) {
		result := slices.Clone(keys)
		return shuffleKeys(result)
	}

	// Near-full unique sampling is better served by partial Fisher-Yates on a clone:
	// it avoids the large sparse map used by offset sampling when K ~ M.
	half := int64(total) / 2
	if int64(total)%2 != 0 {
		half++
	}

	if count >= half {
		result := slices.Clone(keys)
		limit := int(count)

		for i := range limit {
			// Partial Fisher-Yates: only permute the first "limit" positions.

			// #nosec G404 -- math/rand/v2 is intentional for non-cryptographic load-test sampling.
			j := i + rand.IntN(len(result)-i)
			result[i], result[j] = result[j], result[i]
		}

		return result[:limit]
	}

	offsets := sampleUniqueOffsets(total, int(count))
	result := make([]string, 0, len(offsets))

	for _, offset := range offsets {
		result = append(result, keys[offset])
	}

	return result
}

// shuffleKeys returns keys in a uniformly random order.
func shuffleKeys(keys []string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

// sampleKeysWithReplacement draws count keys with replacement from keys.
func sampleKeysWithReplacement(keys []string, count int64) []string {
	result := make([]string, 0, count)

	for range count {
		result = append(
			result,

			// #nosec G404 -- math/rand/v2 is intentional for non-cryptographic load-test sampling.
			keys[rand.IntN(len(keys))],
		)
	}

	return result
}

// sampleUniqueOffsets returns count distinct offsets in [0, total) without cloning keys.
func sampleUniqueOffsets(total, count int) []int {
	if total <= 0 || count <= 0 {
		return []int{}
	}

	if count > total {
		count = total
	}

	offsets := make([]int, 0, count)
	swaps := make(map[int]int, count)

	for i := 0; i < count; i++ {
		// Sample index from [i, total).

		// #nosec G404 -- math/rand/v2 is intentional for non-cryptographic load-test sampling.
		j := i + rand.IntN(total-i)

		valueAtJ, ok := swaps[j]
		if !ok {
			valueAtJ = j
		}

		valueAtI, ok := swaps[i]
		if !ok {
			valueAtI = i
		}

		offsets = append(offsets, valueAtJ)
		swaps[j] = valueAtI
	}

	return offsets
}
