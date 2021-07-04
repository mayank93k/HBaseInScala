package spark.practice.mayank.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.log4j.{Level, Logger}

object hBaseListTables {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Listing of Table Starts")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbasehost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    //**********************Listing HBase Table********************
    val admin = connection.getAdmin
    val tableDescriptors = admin.listTables()
    for (tableDescriptor <- tableDescriptors) {
      println("Table Name: " + tableDescriptor.getNameAsString)
    }
    admin.close()
    connection.close()
    println("Listing of HTable Done")
  }
}
