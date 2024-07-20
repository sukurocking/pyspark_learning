Let's tackle these Spark-related interview questions one by one, keeping the answers concise and straightforward, with code-based solutions where applicable.

1. **Spark Architecture - Job/Stages/Tasks**: 
   - **Job**: A Spark job is a sequence of computations resulting from an action on an RDD (Resilient Distributed Dataset), such as `count()` or `saveAsTextFile()`.
   - **Stages**: Jobs are divided into stages. Stages are split based on transformations that result in shuffling data across partitions (e.g., `reduceByKey()`). Each stage contains tasks based on partitions of the input data.
   - **Tasks**: Tasks are the smallest unit of work in Spark, executed on a single partition of data in a stage. Each task corresponds to a combination of data and computation on that data.

2. **Narrow and Wide Transformations**:
   - **Narrow Transformations**: These transformations do not require data to be shuffled across partitions (e.g., `map()`, `filter()`). Data required to compute the records in a single partition reside in at most one partition of the parent RDD.
   - **Wide Transformations**: These require shuffling of data across many partitions (e.g., `groupBy()`, `reduceByKey()`). They might result in data movement across the cluster.

3. **Hive vs Spark - when to use which?**:
   - Use **Hive** for traditional batch processing tasks, particularly when working with large, stable data sets and SQL-like querying is preferred.
   - Use **Spark** for in-memory data processing to handle real-time analytics, iterative algorithms in machine learning, or when processing data that requires frequent updates.

4. **Persist vs Cache (core difference?)**:
   - Both are used to save intermediate results across operations, but `persist()` allows you to specify the storage level (MEMORY_ONLY, MEMORY_AND_DISK, etc.), whereas `cache()` uses the default storage level of MEMORY_ONLY.

5. **Cache vs Broadcast Join (core difference?)**:
   - **Cache** is used to store RDDs across nodes for faster access in subsequent actions.
   - **Broadcast Join** involves sending a smaller DataFrame to all nodes to optimize join operations with a larger DataFrame, reducing shuffling.

6. **Partitions Overwrite Methods**:
   - Options include `dynamic` partition overwrite, which overwrites only the partitions with new data, and `static` partition overwrite, which overwrites the entire target table.

7. **Shuffle Partitions (default value and how to tune it?)**:
   - The default value is 200. You can tune it by setting `spark.sql.shuffle.partitions` based on the size of data and resources available, aiming to optimize parallelism and memory usage.

8. **How to submit spark jobs**:
   - Use the `spark-submit` command:
     ```
     spark-submit --class YourMainClass --master local[4] your-spark-application.jar
     ```
   Replace `YourMainClass` with your application's main class and `your-spark-application.jar` with your application's JAR file.

9. **How to troubleshoot/debug Spark Job**:
   - Use the Spark Application History UI to check metrics like task execution times, stage durations, and memory usage. Look for tasks that failed and examine their logs for exceptions or errors.

10. **Compression Techniques**:
    - Default is typically `snappy` for its balance between speed and compression ratio.
    - **Snappy** is faster but provides lower compression ratios.
    - **Gzip** provides higher compression ratios but is slower.

11. **Benefits of Spark Adaptive Query Execution (AQE)**:
    - AQE optimizes query plans based on runtime statistics, dynamically coalescing shuffle partitions, optimizing join strategies, and adjusting query stages, leading to faster execution times and more efficient resource utilization.

These answers provide a comprehensive overview of each topic, focusing on the essentials for a clear understanding.  