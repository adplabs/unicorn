import com.adp.cdg._
import com.adp.cdg.DocumentImplicits._
import com.adp.cdg.store._
import com.adp.cdg.store.accumulo._
import com.adp.cdg.store.hbase._

// measure running time of a function/block 
def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  println("time: " + (System.nanoTime - s)/1e6 + " ms")
  ret
}

// connect to Accumulo server
val server = AccumuloServer("local-poc", "127.0.0.1:2181", "tester", "adpadp")
// Use table "small" 
val table = server.dataset("small", "public")

//val server = AccumuloServer("poc", "cdldvtitavap015:2181,cdldvtitavap016:2181,cdldvtitavap017:2181", "tester", "adpadp")
//val server = HBaseServer()
//val table = server.dataset("small")

// Read a non-existing row. It is the pure time of round trip.
val doc = time { "row1" of table }

// Create a document 
val person = Document("293050")
person("name") = "Haifeng"
person("gender") = "Male"
person("salary") = 1.0
person("zip") = 10011

// Create another document 
val address = Document("293050")
address.street = "135 W. 18th ST"
address.city = "New York"
address.state = "NY"
address.zip = person.zip

// add a doucment into another one
person.address = address
// add an array into a document
person.projects = Array("GHCM", "Analytics")

person("work with", "Jim") = true
person("work with", "Mike") = true
person("report to", "Jerome") = true

person.relationships("Jim")
person.neighbors("work with")
person("report to", "Jim")
person("report to", "Jerome")

// save document into a dataset
time { person into table }

// save it again. should be in no time.
time { person into table }

// Read back the document
val haifeng = time { "293050" of table }

// Read partially a document
val partial = time { "293050".from(table).select("name", "gender") }

// Remove a field
partial remove "gender"
partial commit

// Let's check if "gender" was deleted
val onlyname = time { "293050".from(table).select("name", "gender") }

// Restore gender
onlyname.gender = "Male"
onlyname.commit

// Turn on the cache
table cacheOn

val once = time { "293050" of table }
haifeng.name = "Haifeng Li"
haifeng.gender = null
haifeng commit

val twice = time { "293050" of table }
