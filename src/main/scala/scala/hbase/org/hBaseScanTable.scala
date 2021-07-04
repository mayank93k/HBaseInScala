package spark.practice.mayank.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import java.io.IOException
import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.filter.Filter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.CellUtil

object hBaseScanTable {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Scanning of Table Starts")
    val spark = SparkSession
    .builder()
    .appName("HBaseSparkDataFrame")
    .master("local[*]")
    .getOrCreate()

    // Create a new Hbase DB Connection
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbasehost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    val tableName = "employee"
    val table = TableName.valueOf(tableName)
    val getTableName = connection.getTable(table)

    // Initiate a new client scanner to retrieve records
    val scan: Scan = new Scan()

    // Set the filter condition
    var prfxValue = "key"
    val filter: Filter = new PrefixFilter(Bytes.toBytes(prfxValue))
    scan.setFilter(filter)

    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"))
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"))
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"))
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"))

    // Retrieve Records
    val scanner = getTableName.getScanner(scan)


    // Iterate through the results and store the results into resValues Collection
    var resValues: List[Map[String, String]] = List()
    scanner.asScala.foreach(result => {
      var resultMap: Map[String, String] = Map()
      val cells = result.rawCells()
      for (cell <- cells) {
        val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
        val colValue = Bytes.toString(CellUtil.cloneValue(cell))
        resultMap = resultMap ++ Map(col_name -> colValue)
      }
      val resultList = List(resultMap)
      resValues = resValues ::: resultList
    })

    // Releases any resources held.
    scanner.close()
    getTableName.close()
    connection.close()


    // Lets create the dataFrame
    val colValListMap = resValues

    // Get column names from the Map
    val colList = colValListMap.map(x => {
      x.keySet
    })

    // Get all unique columns from the list
    val uniqColList = colList.reduce((x, y) => x ++ y)
    val emptyString = ""

    // Add empty values for the non existing keys
    val newColValMap = colValListMap.map(eleMap => {
      uniqColList.map(col => {
        (col, eleMap.getOrElse(col, emptyString))
      }).toMap
    })

    // create your rows
    val rows = newColValMap.map(m => Row(m.values.toSeq: _*))

    // create the schema from the header
    val header = newColValMap.head.keys.toList
    val schema = StructType(header.map(fieldName =>
      StructField(fieldName, StringType, true)))
    val sc = spark.sparkContext

    // create your rdd
    val rdd = sc.parallelize(rows)

    // create your DataFrame using
    val resultDF = spark.sqlContext.createDataFrame(rdd, schema)
    resultDF.show(false)
  }
}
