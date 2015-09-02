UNICORN
=======

Unicorn is a NoSQL database supporting key-value pairs, documents, graph, and full text search with relevance ranking. Agility, flexibility and easy to use are our key design goals. Besides, we don’t want to reinvent wheels in areas of data storage, network topology, replication, etc. Instead, we designed Unicorn with pluggable storage engine architecture. Currently, Unicorn supports Accumulo, Cassandra, and HBase as the storage backends. With different storage engine, we can achieve different strategies on consistency, replication, etc. Beyond these, Unicorn adds a unified data model and full text search.

Unicorn is implemented in Scala and can be easily accessed in most JVM language. Unicorn also provides a console with DSEL in Scala. With the built-in document and graph data models, developers can focus on the business logic rather than work with tedious key-value pair manipulations. Of course, developers are still free to use key-value pairs for flexibility in some special cases. In what follows, we will show some Unicorn features with code snippets that can be directly applied in Unicorn console.

```scala
val server = CassandraServer("127.0.0.1", 9160)
val db = server.dataset("employee")
```

Here we first create a server object pointing to a backend storage engine. Instead of Cassandra in this example, users can also use HBase or Accumulo. Usually this is the only line to change in order to switch the backend. The second line creates a reference to working dataset. There are also create and drop functions to create or drop datasets, correspondingly.

```scala
// Create a document
val person = Document("myid")
person("name") = "Haifeng"
person("gender") = "Male"
person("salary") = 1.0

// Create another document with dot syntax for assignment
val address = Document("myid")
address.street = "89 Main ST"
address.city = "New York"
address.state = "NY"
address.zip = 10017
 
// add a nested document
person.address = address
// add an array into a document
person.projects = Array("Big Data", "Analytics", "Machine Learning")
 
// save into database. If a document with same key exists,
// it will be updated.
person into db
// retrieve it back
val haifeng = db get "myid"
 
// delete a field
haifeng remove "salary" // or by haifeng.salary = null (or None)
// save it back
haifeng into db
```

We now just create a document which should have a unique id. Like MongoDB, the documents are JSON-like objects including additional types such as date, int, long, double, byte array, etc. The documents contain one or more fields, and each field contains a value of a specific data type (maybe object or array). Because Unicorn is schemaless, fields are not required to be defined as columns in relational databases. Users are free to put any fields into a document. It’s straightforward to save documents or get them back by id. It is also easy to update or delete fields. When saving the document back, only changed/deleted fields will be updated for efficiency. Saving the document without mutations is essentially a nop.

```scala
val partial = Document("myid").from(db).select("name", "zip")
```

Because the data schema is usually denormalized in NoSQL, documents are typically large with many fields. It is not efficient to get the whole documents in many situations because only few fields will be accessed. Unicorn supports to retrieve partial documents as shown in the above, which is known as "projection" in relational database and MongoDB.

```scala
// Add relationships to other persons
person("work with", "Jim") = "Big Data"
person("work with", "Mike") = "Analytics"
person("report to", "Tom") = new Date(2014, 1, 1)
 
// Query relationships to Jim
person.relationships("Jim")
// Query neighbors of given relationship(s)
person.neighbors("work with")
person.neighbors("work with", "report to")
// Gets the value/object associated with the given relationship.
// If it doesn't exist, undefined will be returned.
person("report to", "Jim")
person("report to", "Tom")
```

Beyond documents, Unicorn supports the graph data model. In Unicorn, documents are (optionally) vertices/nodes in a graph, which is permitted to have multiple parallel edges/relationships, that is, edges that have the same end nodes as long as different tags. Each relationship/edge is defined as a tuple (source, target, tag, value), where source and target are documents, tag is the label of relationship, and value is any kind of JSON object associated with the relationship.

It is easy to query the neighbors or relationships as shown in the code snippet. However, it is more interesting to do graph traversal, shortest path and other graph analysis. Unicorn supports DFS, BFS, A* search, Dijkstra algorithm, topological sort and more with user defined functions. The below code snippet shows how to use A* search to find the shortest path between two nodes.

```scala
// GraphOps is a companion class providing major graph algorithms.
val graphOps = new GraphOps[Document, (String, JsonValue)]()
 
// A* search for a shortest path between haifeng and mike
val path = graphOps.astar(haifeng, mike,
  // user defined function which decides
  // what kind of relationship to explore
  (doc: Document) => {
    val neighbors = doc.neighbors("works with", "reports to")
    neighbors.foreach { case (doc, _) => doc.loadRelationships }
    neighbors.iterator
  },
  // user defined function which returns the weight of edges explored.
  (a: Document, b: Document) => (a.rank, b.rank) match {
    case (ar: JsonIntValue, br: JsonIntValue) => math.abs(ar.value - br.value)
    case _ => 100
  },
  // user defined function which returns the heuristic value of
  // the path from the current node to the target
  (a: Document, b: Document, e: (String, JsonValue)) => e._1 match {
    case "works with" => 1.
    case "reports to" => 2.
    case _ => 3.
  }
)
```

In this example, the graph processing is performed on the database directly. This is fine because most nodes will be accessed only once in A* search. However, it will be costly for many advanced graph algorithms that may access nodes or edges multiple times. Unicorn provides DFS and BFS to create in-memory sub-graphs stored in DocumentGraph objects that is more suitable for things like network flow, assignment, coloring, etc.

With the basic features presented so far, it is very easy to add more advanced features in Unicorn. For example, we built the full text search with relevance ranking into Unicorn in only one week (everything homemade, no use of Lucene or Solr).