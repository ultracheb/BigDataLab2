package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
	def main(args: Array[String]) {

		val sc = new SparkContext(new SparkConf().setAppName("Task2"))
		val file = sc.textFile("hdfs://localhost:8020" + args(0))

		val result = file
			.map(line => line.split("\t"))
			.map(lineParts => (lineParts(1), lineParts(2).toInt))
			.reduceByKey((x, y) => x + y)
			.map(tuple => tuple.productIterator.mkString(" "))

		result.saveAsTextFile("hdfs://localhost:8020" + args(1))
	}
}
