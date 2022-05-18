# RDD_programming_basics
<p>Two types of Apache Spark RDD operations are- Transformations and Actions. A Transformation is a function that produces new RDD from the existing RDDs but when we want to work with the actual dataset, at that point Action is performed. When the action is triggered after the result, new RDD is not formed like transformation. </P>
<h2>Transformations</h2>
<table>
<tbody><tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>map</b>(<i>func</i>) </td>
  <td> Return a new distributed dataset formed by passing each element of the source through a function <i>func</i>. </td>
</tr>
<tr>
  <td> <b>filter</b>(<i>func</i>) </td>
  <td> Return a new dataset formed by selecting those elements of the source on which <i>func</i> returns true. </td>
</tr>
<tr>
  <td> <b>flatMap</b>(<i>func</i>) </td>
  <td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td>
</tr>
<tr>
  <td> <b>mapPartitions</b>(<i>func</i>) <a name="MapPartLink"></a> </td>
  <td> Similar to map, but runs separately on each partition (block) of the RDD, so <i>func</i> must be of type
    Iterator&lt;T&gt; =&gt; Iterator&lt;U&gt; when running on an RDD of type T. </td>
</tr>
<tr>
  <td> <b>mapPartitionsWithIndex</b>(<i>func</i>) </td>
  <td> Similar to mapPartitions, but also provides <i>func</i> with an integer value representing the index of
  the partition, so <i>func</i> must be of type (Int, Iterator&lt;T&gt;) =&gt; Iterator&lt;U&gt; when running on an RDD of type T.
  </td>
</tr>
<tr>
  <td> <b>sample</b>(<i>withReplacement</i>, <i>fraction</i>, <i>seed</i>) </td>
  <td> Sample a fraction <i>fraction</i> of the data, with or without replacement, using a given random number generator seed. </td>
</tr>
<tr>
  <td> <b>union</b>(<i>otherDataset</i>) </td>
  <td> Return a new dataset that contains the union of the elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>intersection</b>(<i>otherDataset</i>) </td>
  <td> Return a new RDD that contains the intersection of elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>distinct</b>([<i>numPartitions</i>])) </td>
  <td> Return a new dataset that contains the distinct elements of the source dataset.</td>
</tr>
<tr>
  <td> <b>groupByKey</b>([<i>numPartitions</i>]) <a name="GroupByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable&lt;V&gt;) pairs. <br>
    <b>Note:</b> If you are grouping in order to perform an aggregation (such as a sum or
      average) over each key, using <code>reduceByKey</code> or <code>aggregateByKey</code> will yield much better
      performance.
    <br>
    <b>Note:</b> By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
      You can pass an optional <code>numPartitions</code> argument to set a different number of tasks.
  </td>
</tr>
<tr>
  <td> <b>reduceByKey</b>(<i>func</i>, [<i>numPartitions</i>]) <a name="ReduceByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function <i>func</i>, which must be of type (V,V) =&gt; V. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>aggregateByKey</b>(<i>zeroValue</i>)(<i>seqOp</i>, <i>combOp</i>, [<i>numPartitions</i>]) <a name="AggregateByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>sortByKey</b>([<i>ascending</i>], [<i>numPartitions</i>]) <a name="SortByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean <code>ascending</code> argument.</td>
</tr>
<tr>
  <td> <b>join</b>(<i>otherDataset</i>, [<i>numPartitions</i>]) <a name="JoinLink"></a> </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
    Outer joins are supported through <code>leftOuterJoin</code>, <code>rightOuterJoin</code>, and <code>fullOuterJoin</code>.
  </td>
</tr>
<tr>
  <td> <b>cogroup</b>(<i>otherDataset</i>, [<i>numPartitions</i>]) <a name="CogroupLink"></a> </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable&lt;V&gt;, Iterable&lt;W&gt;)) tuples. This operation is also called <code>groupWith</code>. </td>
</tr>
<tr>
  <td> <b>cartesian</b>(<i>otherDataset</i>) </td>
  <td> When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). </td>
</tr>
<tr>
  <td> <b>pipe</b>(<i>command</i>, <i>[envVars]</i>) </td>
  <td> Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the
    process's stdin and lines output to its stdout are returned as an RDD of strings. </td>
</tr>
<tr>
  <td> <b>coalesce</b>(<i>numPartitions</i>) <a name="CoalesceLink"></a> </td>
  <td> Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently
    after filtering down a large dataset. </td>
</tr>
<tr>
  <td> <b>repartition</b>(<i>numPartitions</i>) </td>
  <td> Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
    This always shuffles all data over the network. <a name="RepartitionLink"></a></td>
</tr>
<tr>
  <td> <b>repartitionAndSortWithinPartitions</b>(<i>partitioner</i>) <a name="Repartition2Link"></a></td>
  <td> Repartition the RDD according to the given partitioner and, within each resulting partition,
  sort records by their keys. This is more efficient than calling <code>repartition</code> and then sorting within
  each partition because it can push the sorting down into the shuffle machinery. </td>
</tr>
</tbody></table>
<h2>Actions</h2>
<table>
<tbody><tr><th>Action</th><th>Meaning</th></tr>
<tr>
  <td> <b>reduce</b>(<i>func</i>) </td>
  <td> Aggregate the elements of the dataset using a function <i>func</i> (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. </td>
</tr>
<tr>
  <td> <b>collect</b>() </td>
  <td> Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. </td>
</tr>
<tr>
  <td> <b>count</b>() </td>
  <td> Return the number of elements in the dataset. </td>
</tr>
<tr>
  <td> <b>first</b>() </td>
  <td> Return the first element of the dataset (similar to take(1)). </td>
</tr>
<tr>
  <td> <b>take</b>(<i>n</i>) </td>
  <td> Return an array with the first <i>n</i> elements of the dataset. </td>
</tr>
<tr>
  <td> <b>takeSample</b>(<i>withReplacement</i>, <i>num</i>, [<i>seed</i>]) </td>
  <td> Return an array with a random sample of <i>num</i> elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.</td>
</tr>
<tr>
  <td> <b>takeOrdered</b>(<i>n</i>, <i>[ordering]</i>) </td>
  <td> Return the first <i>n</i> elements of the RDD using either their natural order or a custom comparator. </td>
</tr>
<tr>
  <td> <b>saveAsTextFile</b>(<i>path</i>) </td>
  <td> Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. </td>
</tr>
<tr>
  <td> <b>saveAsSequenceFile</b>(<i>path</i>) <br> (Java and Scala) </td>
  <td> Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also
   available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). </td>
</tr>
<tr>
  <td> <b>saveAsObjectFile</b>(<i>path</i>) <br> (Java and Scala) </td>
  <td> Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using
    <code>SparkContext.objectFile()</code>. </td>
</tr>
<tr>
  <td> <b>countByKey</b>() <a name="CountByLink"></a> </td>
  <td> Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. </td>
</tr>
<tr>
  <td> <b>foreach</b>(<i>func</i>) </td>
  <td> Run a function <i>func</i> on each element of the dataset. This is usually done for side effects such as updating an <a href="#accumulators">Accumulator</a> or interacting with external storage systems.
  <br><b>Note</b>: modifying variables other than Accumulators outside of the <code>foreach()</code> may result in undefined behavior. See <a href="#understanding-closures-a-nameclosureslinka">Understanding closures </a> for more details.</td>
</tr>
</tbody>
  
</table>

Reference Link <a href='https://spark.apache.org/docs/latest/rdd-programming-guide.html'>link</a>
