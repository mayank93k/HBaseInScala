package spark.practice.mayank.hbase

object tableSchema {
  case class Employee(id: Int, name: String, city: String, designation: String, salary: Int)
}
