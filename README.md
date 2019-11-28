# SF Crime Project

## Question 1

> How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It either increased or decreased `processedRowsPerSecond`


## Question 2
> What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

The options with the most effect was the following, set to the respective values.

The meassure I used to guage the effect was `processedRowsPerSecond` and the highest value I observed was `301.5151515151515`


```
spark.sql.shuffle.partitions                10
spark.streaming.kafka.maxRatePerPartition   10
spark.default.parallelism                   10000
```

