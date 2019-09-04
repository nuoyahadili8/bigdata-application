package test

import org.apache.spark.sql.SparkSession

case class Person(id:Int,name:String) extends Serializable

object Spark2RDD {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val person = new Person(1,"a")

    val personBroadcast = sc.broadcast(person)

    sc.parallelize(1 to 100,10).foreachPartition(partition =>{
      val p = personBroadcast.value
      partition.map(line =>{
        p.id + line
      }).foreach(println)
    })

    sparkSession.stop()
  }

}
