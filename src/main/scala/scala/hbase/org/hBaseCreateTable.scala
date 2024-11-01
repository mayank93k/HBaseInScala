package scala.hbase.org

import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}

import java.io.IOException

object hBaseCreateTable {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Create Table Starts")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbasehost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    //**********************Create HBase Table********************
    val admin = connection.getAdmin
    val table = "employee"
    val tableName = TableName.valueOf(table)
    try {
      val tableDescriptor = TableDescriptorBuilder
        .newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("personal".getBytes).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("professional".getBytes).build())
        .build()

      admin.createTable(tableDescriptor)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      admin.close()
      connection.close()
    }
    println("Creation of HTable Done")
  }
}
