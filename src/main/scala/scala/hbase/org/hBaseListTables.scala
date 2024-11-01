package scala.hbase.org

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, TableDescriptor}
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters.asScalaBufferConverter

object hBaseListTables {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Listing of Table Starts")
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "hbasehost")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    val connection: Connection = ConnectionFactory.createConnection(config)
    val admin: Admin = connection.getAdmin
    //**********************Listing HBase Table********************

    // List all table descriptors
    val tableDescriptors: Seq[TableDescriptor] = admin.listTableDescriptors().asScala

    // Iterate through table descriptors and print table names
    tableDescriptors.foreach { descriptor =>
      println(s"Table: ${descriptor.getTableName}")
    }

    admin.close()
    connection.close()
    println("Listing of HTable Done")
  }
}
