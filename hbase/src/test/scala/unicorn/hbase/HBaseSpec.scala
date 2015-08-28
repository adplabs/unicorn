package unicorn.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.specs2.mutable._

/**
 * Created by lihb on 8/27/15.
 */
class HBaseSpec extends Specification {
  val hbase = HBase()
  val tableName = "unicorn_test"

  trait Context extends BeforeAfter {
    def before: Any = hbase.createTable(tableName, "cf1", "cf2")
    def after: Any = hbase.dropTable(tableName)
  }

  "HBase" should {
    "create table, put, get, drop table" in new Context {
      val table = hbase("unicorn_test")
      table.put("row1", "cf1", "c1", "v1")
      table.get("row1", "cf1", "c1") === Some(Bytes.toBytes("v1"))
    }
  }
}
