
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 3. Design overview of SB-Tree.
FIGURE 4. Block Layouts in SB-Tree.
D. N-ARY SEARCH TABLE
In SB-Tree, a lookup or scan operation is performed on a
data block using a linear search. However, since the size of
data block is 4KB, linear search shows low performance.
Binary search can be an alternative solution, but it incurs
random memory access, which does not efficiently leverage
the memory prefetcher and caches. To take the advantages
of the memory prefetcher and CPU caches, SB-Tree proposes
the N-ary search table. SB-Tree divides each data block into
multiple small buckets and selects the smallest key from each
bucket to construct the N-ary search table. When SB-Tree
finds a key from a data block, SB-Tree first searches the N-ary
search table to determine the bucket where the key might
be stored, and then performs a linear search for that bucket.
This approach improves query performance by minimizing
random memory accesses during search operations.
E. BLOCK ALLOCATOR
System call overhead for allocating and freeing memory
spaces becomes a performance bottleneck in in-memory
data structures. SB-Tree uses its own memory allocator for
allocating its blocks. Since the size of the blocks is the same,
the freed blocks can be easily reused across different types
of allocations. The block allocator allocates a large memory
space at once and slices it into multiple blocks as needed.
Additionally, each thread manages its own memory blocks to
minimize contention.
F. SYNCHRONIZATION
SB-Tree consists of a search layer and a data layer, each with
its own concurrency control mechanism. The search layer
leverages ROWEX [31] by allowing only a single dedicated
thread to insert data into the tree. Thanks to the monotonically
increasing characteristic of time series data, most data is
appended to the segmented block, and explicit sorting is
unnecessary. Meanwhile, reader threads can access the search
layer without locking to locate the appropriate data block,
ensuring that query performance is not degraded. Note that
the intermediate process of adding new data is invisible to
reader threads, as the dedicated thread atomically updates the
number of keys in a node after inserting the new key-value
pair.
In the data layer, data blocks use a variant of version-based
locking protocol [32]. The version-based locking protocol
is primarily used for inserting delayed data. In contrast,
segmented block, which manages per-thread data blocks,
do not require a concurrency mechanism since each thread
inserts its data into its own per-thread data block. When
a per-thread data block becomes full, the segmented block
needs to be converted into data blocks. In this case,
SB-Tree uses version-based locking to prevent additional
insert operations into the segmented block and its associated
per-thread data blocks.
IV. OPERATIONS
A. INSERT OPERATION FOR IN-ORDER DATA
In time series workloads, newly arriving data always
generally has a greater key than the keys in the index
structure. Hence, SB-Tree leverages a shortcut to directly
access the segmented block, as shown in algorithm 1. Since
the shortcut bypasses the search layer, it can efficiently
reduce the tree traversal overhead. Note that accessing the
segmented block is critical for insert operations, as the
segmented block notifies that the per-thread data blocks
VOLUME 13, 2025 144931
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 5. Conversion process from a Segmented block to multiple data blocks in SB-Tree.
Algorithm 1 SB-Tree Insert
1 Procedure Insert(key, value)
2 if ShortcutInsert(DataList, key, value) then
3 return
4 block ← FindBlock(SearchLayer, key)
5 while Covers(block.nextBlock, key) do
6 block ← block.nextBlock
7 InsertIntoBlock(block, key, value)
within the segmented block are being converted into data
blocks.
1) INSERTING AT SEGMENTED BLOCK
In the segmented block, the insert operation checks whether
the current thread already has an allocated per-thread data
block. When there is no per-thread data block for the thread
or the existing block is full, SB-Tree allocates a new one and
assigns it to the segmented block. Otherwise, the data is stored
in the current per-thread data block.
The per-thread data block stores data in the form of key-
value (KV) entries, which are optimized for insert operations.
Storing data as KV entries ensures that inserts occur in
contiguous memory regions. Storing keys and values in
separate contiguous regions would cause each insertion
to access different cache lines, increasing cache usage.
By storing data as KV entries, SB-Tree enables sequential
memory accesses, which reduces memory operations and
improves cache locality.
In SB-Tree, the per-thread data blocks are dynamically
distributed to threads based on demand. The number of
per-thread data blocks is atomically increased and used to get
the location of the pointer variable. Each segmented block has
a maximum threshold of the per-thread data block. When the
number of per-thread data block exceeds this maximum, the
segmented block does not allow further insert operations and
is converted into the multiple data blocks.
B. CONVERTING SEGMENTED BLOCKS TO DATA BLOCKS
When a per-thread data block is full, the segmented block
containing it must be converted. To maintain continuous
insertion during the conversion, a new segmented block
is created and pointed to by the shortcut prior to the
conversion.
Figure 5 illustrates how a segmented block with per-thread
data blocks is converted into multiple data blocks. The
conversion process merges and sorts the KV entries from
the per-thread data blocks managed by a segmented block,
and converts them into multiple data blocks. During the
conversion, a thread initially merges KV entries stored in
per-thread data blocks and sorts them in ascending key order.
Since the keys are monotonically increasing, KV entries
in the per-thread data blocks tend to be partially sorted.
Hence, the sorting overhead across the multiple per-thread
data blocks is not significant. For sort operation, SB-Tree uses
std::sort. The sorted KV entries are divided into multiple
data blocks and appended to the data layer. In the data blocks,
keys and values are stored in separate arrays. This design
improves range query performance, as it allows retrieving a
large number of values with fewer memory accesses.
The conversion process requires a number of memory allo-
cation and deallocation operations. In in-memory indexes, the
memory allocation and deallocation overheads are significant
as these operations involve system calls. To reduce these
overhead, SB-Tree uses its own memory allocator called
the block allocator. The block allocator is optimized for
blocks that are used in SB-Tree. SB-Tree uses fixed-size
memory blocks and reuses freed blocks for future allocations.
By designing blocks to have the same sizes, freed block
memory can be easily recycled for new block allocations.
To minimize memory deallocation overhead, the block
allocator manages per-thread free lists instead of releasing
memory back to the system.
C. INSERTING AT THE SEARCH LAYER
The conversion process creates multiple data blocks and adds
them to the data layer. To efficiently look up the data, the data
144932 VOLUME 13, 2025
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
Algorithm 2 SB-Tree Lookup
1 Procedure Lookup(key)
2 block ← FindBlock(SearchLayer, key)
3 while Covers(block.nextBlock, key) do
4 block ← block.nextBlock
5 return SearchInBlock(block, key)
blocks need to be indexed in the search layer. In SB-Tree,
the search layer and the data layer are separated and they
are asynchronously updated. To improve the concurrency,
SB-Tree employs the ROWEX protocol [31] and uses a
dedicated thread to update the search layer. The dedicated
thread scans the data blocks created by the conversion process
and inserts the minimum key and address of each data block
into the search layer. Since the keys that are newly added to
the search layer also have a monotonic increasing order, the
search layer is built in a bottom-up manner. Note that since
the newly inserted data is appended to the rightmost search
block, the critical section is minimal. Data blocks that have
not yet been inserted into the search layer can still be located
by traversing the data layer similar to a linked list, even if the
search layer points to a neighboring node.
D. INSERTING DELAYED DATA
Delayed data refers to data that does not arrive in order due
to network congestion, scheduling, or other factors. SB-Tree
classifies data that cannot be inserted through the shortcut
as delayed data. The search layer traversal is first performed
to find the appropriate data block for inserting the delayed
data, as shown in algorithm 1. Since the search layer is
updated asynchronously, the search operation may locate the
neighboring node. To address this issue, SB-Tree performs
the data layer traversal after search layer traversal to ensure
that the target data block is found correctly. Once the target
data block is found, the data is inserted in a way that preserves
the sorted order within the data block. Maintaining this sorted
structure is essential for supporting efficient range queries.
When the target data block is full, the thread performs a split
similar to that of a general B+-tree’s split operation.
E. QUERY OPERATION
1) LOOKUP OPERATION
Algorithm 2 presents the pseudocode for the lookup operation
in SB-Tree. The lookup operation performs a search layer
traversal to find the appropriate data block. In the data block,
an N-ary search table is used to determine whether the key
is found, or if it should move to the neighboring data block
to continue the search. When SB-Tree finds the appropriate
data block, it scans the N-ary search table to find the bucket
from which to begin a linear search. By using the N-ary
search table, SB-Tree can access the minimum value of each
bucket with fewer memory references, allowing for more
efficient use of CPU caches and the memory prefetcher.
Algorithm 3 SB-Tree Scan
1 Procedure Scan(startKey, count)
2 block ← FindBlock(SearchLayer, startKey)
3 while Covers(block.nextBlock, startKey) do
4 block ← block.nextBlock
5 while count > 0 and block ̸ = null do
6 scannedValues ← ScanInBlock(block,
startKey, count)
7 results.append(scannedValues)
8 count ← count − |scannedValues|
9 block ← block.nextBlock
10 return results
After determining the target bucket, a linear search is
performed to find the key, and if found, the corresponding
value is returned.
2) SCAN OPERATION
Algorithm 3 shows the pseudocode for the scan operation in
SB-Tree. The scan operation works similarly to the lookup
operation. A search layer traversal is performed to find the
data block where the scan operation starts. Within the data
block, the N-ary search table is used to find the bucket
from which to start a linear search. A linear search is
then conducted to find the minimum value for the scan
range. The values are retrieved sequentially as many as
the query requested. Since values in the data block are
stored contiguously, each memory reference can retrieve
more values. This approach becomes more effective as the
number of values to be retrieved increases. If a single data
block does not contain enough data to satisfy the scan range,
the scan operation continues by moving to the next data block
until the query’s requirements are met.
V. EVALUATION
A. EXPERIMENTAL SETUP
1) EXPERIMENTAL ENVIRONMENT
We conduct all experiments on a server equipped with two
Intel Xeon Gold 5318Y CPUs, each containing 24 physical
cores (48 Hyper-Threads per CPU). The server has 768GB
of DDR4 DRAM. We pin each thread to a specific core,
ensuring that each thread allocates memory on its local
Non-Uniform Memory Access (NUMA) node, similar to
previous work [19]. SB-Tree is implemented in C++. We use
GCC 11.3.1 with the -O3 optimization flag. To accelerate
searches, we utilize Intel Advanced Vector Extensions 512
(AVX-512) SIMD operations to search keys in the tree node
and the N-ary search table in the data block.
2) WORKLOADS
We use a time series workload, as in previous studies
[19], [33]. The time series workload simulates a scenario
where data is concurrently received from 1,024 distributed
VOLUME 13, 2025 144933
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 6. Throughput of in-memory indexes across (a) Insert-only, (b) Lookup, (c) Scan, and (d) Mixed workloads, showing SB-Tree’s superior scalability
in insert-heavy scenarios.
sensors. Both the key and value are composed of 8-byte
integers. The key is composed of a 6-byte timestamp followed
by a 2-byte sensor ID. Before inserting a key into the index,
the timestamp is captured using the RDTSC (Read Time-
Stamp Counter) instruction, ensuring that arriving keys are
monotonically increasing. In our experiments, we measure
the performance of 1 billion operations after building index
structure using 1 billion key-value pairs except for the insert-
only workload. The lookup and scan operation select the
keys uniformly at random. The mixed workload follows the
configuration used in previous work [19], consisting of 50%
inserts, 30% long scans, 10% short scans, and 10% lookups.
A long scan queries 10 to 100 values, while a short scan
queries 5 to 10 values.
B. SCALABILITY EVALUATION
1) INSERT-ONLY
Figure 6 (a) shows the throughput of the insert-only
workload with a varying number of threads. The keys in this
workload are monotonically increasing. SB-Tree outperforms
the other indexes by up to 2.7× compared to Blink-Hash
with 80 threads. SB-Tree provides a segmented block with
per-thread data blocks, which provides a dedicated per-thread
data block for each thread, so there is no contention among
threads. Furthermore, SB-Tree reduces system call overhead
by using its block allocator. During the 1-billion insert work-
load with 80 threads, a total of 36,646 conversions occurred,
averaging approximately 458 conversions per thread. We also
observed that data layer traversals occurred in approximately
0.02% of inserts. On average, these traversals scanned
154 data blocks before locating the target key. Importantly,
since our data blocks are grouped into segmented blocks, this
range typically corresponds to scanning only 1–2 segmented
blocks. Blink-Hash shows good scalability in throughput
because its leaf node is implemented as a hash node, allowing
concurrent insert operations to be distributed evenly across
multiple hash table buckets within the hash node. In contrast,
other indexes suffer from performance bottlenecks because
the monotonically increasing keys are inserted into the
rightmost leaf node of the indexes. Since most indexes do
not allow more than one insert thread for each node, other
insert threads must restart the tree traversal to find the
target leaf node for insertion, leading to further performance
degradation.
2) LOOKUP
In Figure 6 (b), SB-Tree shows relatively lower lookup per-
formance than Blink-Hash, since the leaf nodes in Blink-Hash
employ a hash table structure, referred to as hash nodes. Also,
the hash node can store a large number of key-value pairs, as it
has multiple buckets of large size. This reduces the height
of the tree structure. Other indexes show similar lookup
performance to SB-Tree. Since the timestamp information
is stored as an integer value, the B+-tree variants show
comparable performance to trie-based tree structure. HOT
shows the best performance as it has a trie structure and low
tree height. Blink-Hash also has similar tree height, but it has
more key comparisons.
3) SCAN
Figure 6 (c) shows the scan performance of in-memory
indexes. SB-Tree achieves superior throughput compared to
other indexes. This is because SB-Tree’s data blocks can
store more entries than the leaf nodes of other in-memory
indexes. The large-sized data block is beneficial not only
for the prefetcher but also for reducing the overall tree size.
In addition, SB-Tree separates keys and values into different
arrays, enabling more efficient value retrieval. The N-ary
search table in SB-Tree also accelerates the lookup of the
minimum value in the scan range.
For the scan evaluation, we use two variants of Blink-Hash:
one in which hash nodes are converted into tree nodes during
scan operations (baseline), and another in which all hash
nodes are converted before the scan operations (pre-converted
hash nodes). In our experiments, the configuration with
pre-converted hash nodes achieves only 0.86× the average
performance of the baseline configuration. This performance
degradation is due to the fact that converting all hash nodes
in advance results in a taller tree, which increases the number
of memory references during traversal. Blink-tree shows better
performance than trie-based trees such as HOT and Masstree.
Masstree shows the worst performance because it has higher
144934 VOLUME 13, 2025
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 7. Single-threaded throughput in time series workload.
tree height than other indexes. Also, Masstree requires more
node visits.
4) MIXED
Figure 6 (d) represents the performance for the mixed
workload. SB-Tree and Blink-Hash exhibit scalable perfor-
mance as the number of threads increases. Since 50% of the
mixed workload consists of insert operations, other indexes
suffer from performance bottleneck. The keys are sorted and
monotonically increasing, causing insert operations to be
skewed toward a specific node.
C. SINGLE-THREADED PERFORMANCE
1) INSERT-ONLY
In Figure 7 (a), SB-Tree achieves up to 7× better performance
than other indexes because SB-Tree directly accesses the
segmented block via a shortcut. SB-Tree also achieves a
high cache hit rate, leading to fast insertion performance.
Other indexes show lower performance because they need
to traverse the tree to search for the appropriate node
for insertion and perform structural modification operation.
Blink-Hash shows similar performance to Blink-tree due to its
split operation. In Blink-Hash, the split operation first sorts the
key-value pairs and divides them into two nodes. To reduce
the performance overhead, it proposes median approximation
and a lazy split operation. HOT shows lower throughput than
other indexes because it incurs more structural modification
operations to maintain the minimum tree height. Blink-tree
shows comparable performance to Blink-Hash although it
has higher tree height, because Blink-tree’s split operation
is simpler than that of Blink-Hash. Masstree shows better
performance than Blink-tree and Blink-Hash since it uses a
combination of a trie and B+-tree structure.
2) LOOKUP
Figure 7 (b) illustrates the lookup performance of indexes in
a single-threaded environment. SB-Tree shows comparable
lookup performance to other indexes even though it has
large-sized data blocks. Since SB-Tree builds the search layer
in a compact manner, each search block is fully utilized.
Blink-Hash shows 1.1× better performance than SB-Tree
because it has hash nodes. Moreover, it has a lower tree
height as the hash table-based leaf node can store more
key-value pairs. HOT outperforms other indexes by 1.33×
because it dynamically adjusts each node’s span based on the
data distribution, maintaining minimal tree height to maxi-
mize cache efficiency. Blink-tree shows lower performance
than Blink-Hash and SB-Tree because it has higher tree height.
Masstree also has the same tree height as Blink-tree but it
shows better performance thanks to its trie structure.
3) SCAN
In Figure 7 (c), the variants of B+-tree, SB-Tree, Blink-Hash,
and Blink-tree outperform trie-based indexes, HOT and
Masstree. Since the variants of B+-tree store similar
key-value pairs in a sequential manner, they can take
advantage of CPU cache efficiency. SB-Tree shows superior
performance because it has lower tree height and large, sorted
data blocks. SB-Tree also separates keys and values to make
data blocks more CPU cache and prefetch friendly.
For fair comparison, we also measure the performance
of both versions of Blink-Hash: (1) when hash nodes are
dynamically converted into tree nodes during scan operations,
and (2) after all hash nodes have been converted into tree
nodes. In the latter case, the tree height is increased. In our
experiment, the dynamic conversion case shows 0.98× the
performance of the other because all hash node conversions
are handled by a single thread.
D. INSERT PERFORMANCE WITH DELAYED DATA
In this experiment, we measure the performance of indexes
with delayed data. To simulate delayed data, we first build the
indexes by inserting 1 billion timestamp keys and generate
delayed data by uniformly sampling the keys used during
the index-building phase. This approach ensures that the
delayed data is not skewed and is evenly distributed across
multiple nodes. The uniform sampling method used to
generate delayed data represents a worst-case scenario in
terms of locality, as it allows keys to be inserted far from
the current insertion position. While this does not accurately
reflect real-world delay distributions, it provides a more
consistent and fair baseline for comparing different index
structures.
Figure 8 presents the index performance with delayed data.
SB-Tree shows decreasing performance as the proportion of
delayed data increases. In our experiment, SB-Tree shows up
to 0.79× decreases in the insert-only workload and a 0.4×
decrease in the mixed workload. This is because SB-Tree is
optimized for monotonically increasing keys.
In particular, the shortcut in SB-Tree becomes inefficient
when inserting delayed data, as it follows conventional B+-
tree traversal. Furthermore, insert operations to the same data
block are serialized. In our experiment, SB-Tree begins to
show lower performance than Blink-Hash when the proportion
of delayed data exceeds 30%. Note that the data distribution
will be similar to a random distribution when the proportion
of the delayed data is increasing. Blink-Hash distributes inser-
tions across multiple buckets within a hash node, enabling
concurrent insertions. This maintains scalable performance
VOLUME 13, 2025 144935
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 8. Throughput under delayed data (0–50%) with 80 threads,
showing SB-Tree performance degradation beyond 30% due to reduced
shortcut effectiveness.
even in the insert-only workload with delayed data. Other
indexes show better performance as the proportion of delayed
data increases in both the insert-only and mixed workloads.
Specifically, in both workloads, Blink-Hash improves by an
average of 1.03×, Blink-tree by 1.13×, HOT by 1.14×, and
Masstree by 1.31×. However, Blink-tree, HOT, and Masstree
still show low performance because they still suffer from
skewed insertions.
E. PERFORMANCE BREAKDOWN
We analyze the performance of each operation in SB-Tree
while varying the number of threads as shown in Figure 9.
‘‘Search Layer’’ and ‘‘Data Layer’’ in Figure 9 represent the
traversal time for each respective layer. In insert operations,
most of the time is consumed by the insert operation using
the shortcut mechanism, as shown in Figure 9 (a). The
traversal time for the data layer increases as the number of
threads increases, because the search layer is asynchronously
updated with a dedicated thread. As the number of threads is
increasing, the insertions to the search layer can be delayed.
The time for the conversion operation from per-thread data
blocks to the data blocks is reduced as the number of threads
increases.
In lookup and scan operations, the ratio of time
spent on each component remains constant, as shown in
Figure 9 (b) and (c). The lookup operation spends about 40%
of the time for traversing the search layer and about 27% of
the time searching the N-ary search table. In scan operations,
the traversal time for data layer accounts for 30% of the total
execution time.
F. LATENCY ANALYSIS
Figure 10 illustrates the cumulative distribution function
(CDF) of latency for each operation under the time series
workload.
In the insert-only workload, SB-Tree achieves remarkably
low latency for the 99.9th percentile because SB-Tree utilizes
FIGURE 9. Execution time breakdown of SB-Tree: (a) Insert dominated by
shortcut, (b) Lookup with N-ary search and data traversal, (c) Scan
highlighting data layer efficiency.
shortcuts to directly access segmented blocks. When a
per-thread data block has already been allocated within
the corresponding segmented block, the insert is executed
immediately. This mechanism leads to a high cache hit
ratio and low latency. However, SB-Tree exhibits higher
99.99th percentile latency compared to other indexes. This
is due to a single thread being responsible for converting
a segmented block into data blocks, which increases tail
latency. However, in the case of inserts, SB-Tree mitigates
144936 VOLUME 13, 2025
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
FIGURE 10. Latency CDFs at 80 threads for inserts, lookups, and scans.
blocking by allocating additional segmented blocks and by
provisionally over-allocating per-thread data blocks. This
allows other threads to continue their insert operations with
minimal interference, even when one thread is performing a
conversion.
Blink-tree and Masstree show high latency because they
are not optimized for time series data. The monotonically
increasing keys decrease the node utilization. HOT shows
the highest latency because of its structural modification
operation to maintain the minimum tree height.
In the lookup workload, Blink-Hash achieves the lowest
latency for the 50th, 90th, and 95th percentiles thanks
to its hash nodes. The hash nodes enable efficient key
lookups using hash functions and fingerprints within the
nodes, resulting in fast average-case performance. However,
SB-Tree shows superior tail latency performance, achiev-
ing 1.78× and 2.42× lower latency than Blink-Hash at
the 99.9th and 99.99th percentiles, respectively. This is
because SB-Tree’s structural modification operation is sim-
pler than that of Blink-Hash and is done asynchronously.
Masstree shows high latency because it has the highest tree
height.
In the scan workload, SB-Tree achieves the lowest
latency for the 99.9th percentile. Blink-Hash suffers from
an extremely high tail latency, reaching 266.61× that of
SB-Tree. When a thread encounters a hash node during the
scan operation, the thread must convert the node into a tree
node by itself. This conversion significantly increases the tail
latency for scan operations in Blink-Hash. Trie-based indexes
such as HOT and Masstree show high latencies in the scan
workload because of their unbalanced and non-contiguous
structure.
G. MEMORY USAGE
Figure 11 shows the memory usage of each index. The
measurement was taken after inserting 1 billion key-value
pairs from the time series workload. We also present the
absolute index sizes in GB in Table 1. While Figure 11
illustrates relative memory usage patterns, Table 1 provides
the exact sizes, enabling a more precise comparison.
FIGURE 11. Memory usage breakdown after 1B inserts.
Following [19], we classify memory usage into five
parts to analyze SB-Tree efficiency: key data occupied, key
data unoccupied, structural data occupied, structural data
unoccupied, metadata. (1) structural data refers to the keys
and pointers stored in internal nodes. (2) key data represents
the keys and values stored in leaf nodes. (3) metadata
includes additional per-node information beyond keys and
pointers. Unoccupied refers to the portion of memory that
has been allocated but not used, while occupied indicates
the portion that is actively in use. SB-Tree shows an average
of 0.8× lower memory usage compared to variants of
B+-tree including Blink-Hash. The largest reduction appears
in the structural data, particularly due to the memory usage
of SB-Tree’s search layer. Compared to other B+-tree-based
indexes, SB-Tree’s structural data occupied is only about
0.11×, and its structural data unoccupied is 0.004×. This is
because the search layer is constructed by inserting data in a
bottom-up manner, fully filling each tree node, resulting in
VOLUME 13, 2025 144937
C. E. Jung et al.: SB-Tree: A B+-Tree for In-Memory Time Series Databases With Segmented Block
TABLE 1. Memory usage breakdown (absolute size in GB and percentage of total) after inserting 1B key–value pairs.
a compact tree structure. Moreover, since data blocks in the
data layer can store a large number of keys, the number of
keys required in the search layer is also reduced. Blink-Hash
shows the highest memory usage among all indexes due
to its large hash node structure at the leaf level. Its key
data unoccupied alone accounts for approximately 49.57%
of the total memory usage and is 2.43× larger than that of
Blink-tree, because each hash node contains multiple buckets,
many of which remain partially empty. After converting all
hash nodes into tree nodes, the memory usage becomes
comparable to Blink-tree. In this case, Blink-Hash fills tree
nodes up to 80% to enhance memory utilization, resulting in
its key data unoccupied being 0.57× that of Blink-tree. HOT
achieves the lowest memory usage among all indexes. As a
trie-based structure, its key data occupied is only 0.57× that
of other indexes. By dynamically resizing nodes based on the
number of stored entries, it maintains a minimal tree height,
stores data compactly, and uses memory efficiently. Masstree
manages two types of nodes, interior and border nodes, due to
its trie structure composed of multiple B+-trees. Both types
are variants of B+-tree nodes. Interior nodes serve as typical
internal nodes, while Border nodes store key-value entries
and can also point to lower levels, effectively functioning as
both leaf and internal nodes. As a result, Masstree’s Structural
data occupied is on average 2.55× higher than other indexes.
Additionally, Border nodes manage lots of metadata such as
key lengths, suffixes, and permutation data for sorted access,
leading to metadata usage that is 4.91× higher than other
indexes.
VI. DISCUSSION
A. LIMITATIONS OF SB-TREE
1) STALE SEARCH LAYER
Under high-contention workloads, SB-Tree can suffer from
stale search layers, which degrade insert performance. As the
number of threads increases, the conversion process may take
longer, and the resulting increase in data blocks can introduce
additional overhead during data layer traversal.
2) SORTING DURING CONVERSION
When the number of threads increases, the number of
keys that need to be sorted during conversion also grows.
Currently, SB-Tree employs a single thread for sorting, based
on the assumption that each worker thread handles one
index operation independently. This may lead to high tail-
latency, and parallel merge-and-sort support could alleviate
this issue. However, it would require allocating additional
dedicated threads for conversion. While this is technically
feasible, we chose to avoid such parallelism to maintain
a simple per-thread execution model and minimize thread
coordination overhead.
B. GENERALITY OF SB-TREE
1) RANDOM KEYS AND VARIABLE LENGTH KEYS
SB-Tree is optimized for time series workloads where keys
are mostly increasing. Under non-monotonic workloads,
SB-Tree falls back to the standard B+-tree algorithm.
Similarly, for variable-length keys, SB-Tree inherits the
original B+-tree’s insertion and lookup algorithm, and thus
may exhibit suboptimal performance in such cases.
2) SB-TREE WITH PERSISTENT MEMORY TECHNOLOGIES
SB-Tree is designed for in-memory time series databases
and, as such, does not provide crash consistency; the
index must be rebuilt after a system crash. However,
when used with persistent memory technologies such as
NVDIMM-N [34] and CMM-H [35], SB-Tree can be
consistent across system crashes and used efficiently without
requiring a full rebuild. Because time series workloads
typically involve monotonically increasing keys, the keys are
appended to data blocks rather than inserted randomly. This
eliminates the need to re-sort existing data during writes,
resulting in minimal persistence overhead.
3) APPLICABILITY OF SB-TREE
Index structures play an important role in modern systems.
Thus, SB-Tree can be employed as an index structure in
various environments. For example, SB-Tree can be applied
to sensor data management in edge devices [36] and to index
structures of embedded database systems [37]. In addition,
it can be utilized in key-value stores such as RocksDB and
LevelDB [38], [39].
VII. CONCLUSION
In this paper, we proposed SB-Tree, an efficient in-memory
index structure tailored for time series workloads. SB-Tree