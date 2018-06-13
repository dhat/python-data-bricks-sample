// To use this script run from one directory up as follows:
//
// run with: spark-shell -i python-data-bricks-sample/get_avg_ages.scala
//
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val parqfile = sqlContext.read.parquet("python-data-bricks-sample/titanic.parquet")
parqfile.registerTempTable("titanic")
val avgage = sqlContext.sql("SELECT Avg(CAST(Age AS Double)),Count(*) FROM titanic")
val avgcount = sqlContext.sql("SELECT CAST(Count(Age) AS Double) FROM titanic WHERE Age NOT IN ('NA')")
val getvals = (0.75 * avgcount.first.getDouble(0)).toInt
val query  = s"SELECT Avg(Age),Count(*) FROM ( SELECT CAST(Age AS DOUBLE) From titanic WHERE Age NOT IN (\'NA\') ORDER BY Age DESC LIMIT $getvals)"
val top75pctavg = sqlContext.sql(query )
println()
println("Average Age is "+ avgage.first.getDouble(0))
println("Top 75 Percent Average Age is "+ top75pctavg.first.getDouble(0))
System.exit(0)
