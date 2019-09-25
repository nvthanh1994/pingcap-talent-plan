package main

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"sync"
	"time"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	//copy(src, mergeSort(src))
	copy(src, ParallelMergeSort(src))
}

// run MergeSorc on src []int64
func mergeSort(src []int64) []int64 {
	n := len(src)

	// small case
	if n < 32 {
		return insertSort(src)
	}

	// Split input to two equal-parts
	mid := n / 2

	// Run MergeSort on each part
	first := mergeSort(src[:mid])
	second := mergeSort(src[mid:])

	// Merge two sub-array using a temporary array
	return merge(first, second)
}

//merge can be parallelize too
//each element of first, start a goroutine, find its rank on B
//for each x in first, second, result[rank_first(x) + rank_second(x)] = x
//parallel merge only with len(first), len(second) large enough
func merge(first []int64, second []int64) []int64 {
	m := len(first)
	n := len(second)

	result := make([]int64, m+n)
	p1 := 0
	p2 := 0
	i := 0
	for p1 < m && p2 < n {
		// Pickup smaller elements
		if first[p1] <= second[p2] {
			result[i] = first[p1]
			p1++

		} else {
			result[i] = second[p2]
			p2++
		}
		i++

		// Move remain element of either first or second
		if p1 == m {
			for j := p2; j < n; j++ {
				result[i] = second[j]
				i++
			}
		} else if p2 == n {
			for j := p1; j < m; j++ {
				result[i] = first[j]
				i++
			}
		}
	}
	return result
}

func insertSort(numbers []int64) []int64 {
	for i := 0; i < len(numbers); i++ {
		for j := 0; j < i+1; j++ {
			if numbers[j] > numbers[i] {
				// swap
				intermediate := numbers[j]
				numbers[j] = numbers[i]
				numbers[i] = intermediate
			}
		}
	}
	return numbers
}

func ParallelMergeSort(src []int64) []int64 {
	// TODO LIMIT to be a factor of len(src)
	LIMIT := 5000
	return parallelMergeSort(src, LIMIT)
}

func parallelMergeSort(src []int64, LIMIT int) []int64 {
	n := len(src)
	if n <= 1 {
		return src
	}
	// Split input to two equal-parts
	mid := n / 2

	wg := sync.WaitGroup{}
	wg.Add(2)

	var l, r []int64

	// Run MergeSort on each part
	go func() {
		defer wg.Done()
		if n < LIMIT {
			l = mergeSort(src[:mid])
		} else {
			l = parallelMergeSort(src[:mid], LIMIT)
		}

	}()

	go func() {
		defer wg.Done()
		if n < LIMIT {
			r = mergeSort(src[mid:])
		} else {
			r = parallelMergeSort(src[mid:], LIMIT)
		}

	}()
	wg.Wait()

	return merge(l, r)
}

func main() {
	// Init array to be sorted
	rand.Seed(time.Now().Unix())
	start := time.Now()
	src := make([]int64, 1)
	for i := range src {
		src[i] = int64(rand.Int() % 1000)
	}

	// Sort it!
	MergeSort(src)
	fmt.Println(src)
	fmt.Print(len(src), time.Since(start))
}
