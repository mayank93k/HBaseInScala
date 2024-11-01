package scala.hbase.org

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.hbase.org.tableSchema.Employee


object hBasePopulateTable {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Insertion of data to HTable Starts")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbasehost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    //**********************Insert Record into Hbase table********************
    val tableName = "employee"
    val table = TableName.valueOf(tableName)
    val HbaseTable = connection.getTable(table)

    //Lets set the column Families
    val cfPersonal = "personal"
    val cfProfessional = "professional"
    val spark = SparkSession.builder().appName("Age").master("local").getOrCreate()
    val loadData = spark.sparkContext.textFile("/home/mayank/Downloads/HBase/resource/hbaseTest")

    val records = loadData.map(line => {
      val row = line.split(",")
      val id = row(0).toInt
      val name = row(1)
      val city = row(2)
      val designation = row(3)
      val salary = row(4).toInt
      Employee(id, name, city, designation, salary)
    }).collect().toList

    //Iterate through each record to insert
    records.foreach(row => {
      //Prepare column values
      val keyValue = "key_" + row.id
      val transRec = new Put(Bytes.toBytes(keyValue))
      val name = row.name
      val city = row.city
      val salary = row.salary.toString
      val designation = row.designation

      //**********Add the specified column and value, with the specified timestamp as its version to this put operation********
      //Add name to the personal column Family
      transRec.addColumn(Bytes.toBytes(cfPersonal), Bytes.toBytes("name"), Bytes.toBytes(name))
      //Add city to the personal column Family
      transRec.addColumn(Bytes.toBytes(cfPersonal), Bytes.toBytes("city"), Bytes.toBytes(city))

      //Add designation to the professional column Family
      transRec.addColumn(Bytes.toBytes(cfProfessional), Bytes.toBytes("designation"), Bytes.toBytes(designation))
      //Add salary to the professional column Family
      transRec.addColumn(Bytes.toBytes(cfProfessional), Bytes.toBytes("salary"), Bytes.toBytes(salary))

      //Insert record into Hbase
      HbaseTable.put(transRec)
    })
    //Close Hbase table thread to Releases any resource held or pending changes in internal buffers.
    HbaseTable.close()
    connection.close()
    println("Insertion of data to HTable is done")
  }
}
