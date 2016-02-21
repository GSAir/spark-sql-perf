#!/bin/bash

function dspark {
	args=( "$@"  )
	$SPARK_HOME/bin/spark-submit --master local[${args[0]}] --jars ~/.ivy2/cache/com.google.guava/guava/jars/guava-11.0.2.jar,$DELITE_PLAY/target/scala-2.11/delite-playground-assembly-0.1.jar,$SPARK_HOME/assembly/target/scala-2.11/spark-assembly-1.6.0-hadoop2.2.0.jar --class ${args[1]} $SPARK_PERF/target/scala-2.11/spark-sql-perf_2.11-0.3.3-SNAPSHOT.jar ${args[@]:2}
}

dspark "*" Test
