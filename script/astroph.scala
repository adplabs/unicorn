/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

import scala.io.Source

def astroph(server: DataStore, table: DataSet, files: String*): Unit = {
  files.foreach { file =>
    var doc: Document = null
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val edge = line.split("\t")
        if (doc == null) doc = Document(edge(0))
        if (edge(0) != doc.id) {
          println(doc.id)
          doc into table
          doc = Document(edge(0))
        } else {
          doc("works with", edge(1)) = true
        }
      }
    }
  }
}

val server = CassandraServer("127.0.0.1", 9160)
server.createDataSet("astroph")
val table = server.dataset("astroph")

astroph(server, table, "/Users/lihb/data/ca-AstroPh.txt")
