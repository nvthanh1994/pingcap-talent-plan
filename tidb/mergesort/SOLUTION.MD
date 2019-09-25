I just implement normal merge sort with some optimizations

First, we have 3 big function: insertSort, mergeSort, parallelMergeSort - main function

1. When size of input small  (< 32)  using insertSort()

2. Trying to run parallel by:

* When input size < 5000, call normal mergeSort() 
* When input size > 5000, run merge sort of this part on other goroutine by calling parallelMergeSort()

I've limited the number of goroutine, so goroutine will not overused, which lead to slower running time  


Result:

* Merge Sort vs Sort.Slice
BenchmarkMergeSort-8                   1        2837528190 ns/op        2684355304 B/op  1048579 allocs/op
BenchmarkMergeSort-8                   1        2853890567 ns/op        2684356672 B/op  1048581 allocs/op
BenchmarkMergeSort-8                   1        2873332751 ns/op        2684354656 B/op  1048576 allocs/op
BenchmarkNormalSort-8                  1        3244413526 ns/op              64 B/op          2 allocs/op
BenchmarkNormalSort-8                  1        3557203373 ns/op              64 B/op          2 allocs/op
BenchmarkNormalSort-8                  1        3320999013 ns/op              64 B/op          2 allocs/op


* Merge Sort with goroutine vs Sort.Slice 
BenchmarkMergeSort-8                   2        1097071974 ns/op        2685284232 B/op  1076008 allocs/op
BenchmarkMergeSort-8                   1        1013879114 ns/op        2685313456 B/op  1076314 allocs/op
BenchmarkMergeSort-8                   1        1022405469 ns/op        2685255600 B/op  1075708 allocs/op
BenchmarkNormalSort-8                  1        3223513172 ns/op              64 B/op          2 allocs/op
BenchmarkNormalSort-8                  1        3253638040 ns/op              64 B/op          2 allocs/op
BenchmarkNormalSort-8                  1        3201551148 ns/op              64 B/op          2 allocs/op
