UNICORN
=======

Unicorn is a simple and flexible abstraction of BigTable-like database such as Cassandra,
HBase, and Accumulo. Beyond a unified interface to various database systems,
Unicorn provides easy-to-use document data model and MongoDB-like API.
Moreover, a graph data model is implemented as a natural extension to document model.
Even more, full text search is provided with relevance ranking.
Agility, flexibility and easy to use are our key design goals.
With different storage engine, we can achieve different strategies on
consistency, replication, etc.

With the built-in document and graph data models, developers
can focus on the business logic rather than work with tedious
key-value pair manipulations. Of course, developers are still
free to use key-value pairs for flexibility in some special cases.

Unicorn is implemented in Scala and can be used as a client-side library
without overhead. Unicorn also provides a shell for quick access of database.
The code snippets in this document can be directly run in the Shell.
A REST API, in the module Rhino, is also provided to non-Scala users.

To use Unicorn as a library, add the following to SBT build file.

```scala
  libraryDependencies += "com.github.haifengl" % "unicorn-unibase_2.11" % "2.0.0"
```

If you need additional HBase-only features, use

```scala
  libraryDependencies += "com.github.haifengl" % "unicorn-narwhal_2.11" % "2.0.0"
```

Unicorn also has primarily SQL support (simple SELECT only).

```scala
  libraryDependencies += "com.github.haifengl" % "unicorn-sql_2.11" % "2.0.0"
```

Download
========

Get pre-packaged Unicorn in universal tarball from the
[releases page](https://github.com/haifengl/unicorn/releases).

If you would like to build Unicorn from source, please first
install Java, Scala and SBT. Then clone the repo and build the package:

```bash
  git clone https://github.com/haifengl/unicorn.git
  cd unicorn
  ./unicorn.sh
```

which also starts the Shell.

Unicorn runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS).
All you need is to have Java installed on your system `PATH`,
or the `JAVA_HOME` environment variable pointing to a Java installation.

Shell
=====

Unicorn comes with an interactive shell. In the home directory of Unicorn, type

```bash
  ./bin/unicorn
```

to enter the shell, which is based on Scala interpreter. So you can run
any valid Scala expressions in the shell. In the simplest case, you can
use it as a calculator. Besides, all high-level Unicorn operators are
predefined in the shell. Be default, the shell uses up to 4GB memory.
If you need more memory to handle large data, use the option `-J-Xmx`.
For example,

```bash
  ./bin/unicorn -J-Xmx8192M
```

You can also modify the configuration file `./conf/application.ini` for
the memory and other JVM settings.

In the shell, type :help to print Scala interpreter help information.
To exit the shell, type :quit.

Connecting to Database
======================

Suppose that `hbase-site.xml` and in `hbase-default.xml` can be found on the `CLASSPATH`,
one can connect to HBase as simple as

```scala
val db = HBase()
```

The user may also pass a Configuration object to `HBase()`
if HBase configuration files are not in the `CLASSPATH`.

To connect to Cassandra, please enables Thrift by configuring
`start_rpc` to `true` in the `cassandra.yaml` file, which is `false` by default
after Cassandra 2.0.

```scala
val db = Cassandra("127.0.0.1", 9160)
```

To connect to Accumulo,

```scala
val db = Accumulo("instance", "zookeeper", "user", "password")
```

where the first parameter is the Accumulo instance name and the second
is the ZooKeeper connection string.

An interesting feature of Accumulo is to create a mock instance
that holds all data in memory, and will  not retain any data
or settings between runs. It presently does not enforce users,
logins, permissions, etc. This is very convenient for test.
This doesn't even require an installation of Accumulo. To create
a mock instance, simply do

```scala
val db = Accumulo()
```

With a database instance, we can create, drop, truncate, and compact
a table. We can also test if a table exists.

```scala
db.createTable("test_table", "cf1", "cf2")
val table = db("test_table")
table("row1", "cf1", "c1") = "data"
db.dropTable("test_table")
```

Here we first create a table with two column families.
We can also pass a `Properties` object for additional table
configuration, which usually depends on the backend system.
For example, sharding and replication strategies, compression,
time-to-live (TTL), etc.
Then we get the table object, put some data, and finally drop the table.
In what follows, we will go through the data manipulation APIs.

BigTable-like API
=================

In BigTable-like databases, data are stored in tables, which are made
of rows and columns. Columns are grouped into column families.
A column name is made of its column family prefix and a qualifier.
The column family prefix must be composed of printable characters.
The column qualifiers can be made of any arbitrary bytes.
In Cassandra and HBase, column families must be declared up front at schema
definition time whereas new columns can bed added
to any column family without pre-announcing them.
In contrast, column family are not static in Accumulo and can be
created on the fly. The only way to get a complete set of columns
that exist for a column family is to process all the rows.

A cell’s content is an uninterpreted array of bytes. And table
cells are versioned. A `(row, column, version)` tuple exactly specifies
a cell. The version is specified using a long integer. Typically this
long contains timestamps.

The trait `unicorn.bigtable.BigTable` defines basic operations on
a table such as `Get`, `Put`, and `Delete`. The corresponding
implementations are in `unicorn.bigtable.cassandra.CassandraTable`,
`unicorn.bigtable.hbase.HBaseTable`, and `unicorn.bigtable.accumulo.AccumuloTable`.

```scala
  /** Get one or more columns of a column family. If columns is empty, get all columns in the column family. */
  def get(row: ByteArray, family: String, columns: ByteArray*): Seq[Column]

  /** Get all columns in one or more column families. If families is empty, get all column families. */
  def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Seq[ColumnFamily]

  /** Upsert a value. */
  def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit

  /** Upsert values. */
  def put(row: ByteArray, family: String, columns: Column*): Unit

  /** Upsert values. */
  def put(row: ByteArray, families: Seq[ColumnFamily] = Seq.empty): Unit

  /** Delete the columns of a row. If columns is empty, delete all columns in the family. */
  def delete(row: ByteArray, family: String, columns: ByteArray*): Unit

  /** Delete the columns of a row. If families is empty, delete the whole row. */
  def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Unit
```

Without specifying the version, `Put` always creates a new version
of a cell with the server’s currentTimeMillis (for Cassandra, it is
caller's machine time). But the user may specify the version
on a per-column level. The user-provided version may be a time
in the past or the future, or a non-time purpose long value.

```scala
  def update(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit
```

The helper function `update` is provided as a syntactic sugar
so that the user can put a value in the way

```scala
  table(row, family, column) = value
```

In this case, timestamp is always set as the current machine time.

`Delete` can happen on a specific version of a cell or all versions.
Deletes work by creating tombstone markers. Once a tombstone marker
is set, the “deleted” cells become effectively invisible for `Get`
operations but are not immediately removed from store files.
Note that there is a snag with the tombstone approach, namely
“deletes mask puts”. Once a tombstone marker is set, even puts
after the delete will be masked by the delete tombstone.
Performing the put will not fail. However when you do a `Get`,
the `Put` has no effect but will start working  after the major
compaction, which will really remove deletes and tombstone markers.

For these APIs, there are also corresponding batch mode operations
that work on multiple rows. However, the implementation
may or may not optimize the batch operations.
In particular, Accumulo does optimize it in parallel.

Besides the basic operations, advanced features that are not available
on all backend systems are organized into various traits.

Time Travel
-----------

By default, when doing a `Get`, the cell whose version has the
largest value is returned. It is possible to return more than
one version or to return versions other than the latest. These
special methods are defined in trait `TimeTravel`, which is supported
by HBase.

Scan
----

HBase and Accumulo support the `Scan` operation that fetches zero or
more rows of a table. In fact, A `Get` is simply a `Scan` limited by
the API to one row in these systems. The trait `RowScan` provides
operations to scan the whole table, or a range specified by the start
and stop row key, or rows with a given prefix.

Filter Scan
-----------

HBase provides advanced filtering functionality when reading data
using `Get` or `Scan` operations, which return a subset of results
to the client. While this does not reduce server-side IO, it does
reduce network bandwidth and reduces the amount of data the client
needs to process.

The filter operators (`Equal`, `NotEqual`, `Greater`, `GreaterOrEqual`,
`Less`, `LessOrEqual`) and logic operators (`And`, `Or`) are defined
in trait `ScanFilter`. The enhanced `Get` and `Scan` operators with
filter parameter are defined in `FilterScan`.

Intra Row Scan
--------------

In BigTable, a row could have millions columns. Cassandra actually
supports up to 2 billions columns. In such a wide columnar environment,
intra row scan is an useful operation in some use cases. Both HBase
and Cassandra support `IntraRowScan` trait that can scan columns of
given range in a row (inside a column family).

Rollback
--------

`HBaseTable` implements the `Rollback` trait that defines the methods to
rollback cell(s) to previous version.

Counter
-------

A counter is a special column used to store a 64 bit integer that is changed
in increments. Both HBase and Cassandra support counters.

To use counters in Cassandra, the user has to define a column family
whose columns will act as counters.

Append
------

HBase support the `Append` operation that appends data to a cell.
Note that this operation does not appear atomic to readers. Appends
are done under a single row lock, so write operations to a row are
synchronized, but readers do not take row locks so get and scan operations
can see this operation partially completed.

Cell Level Security
-------------------

Accumulo and HBase support cell level security that provides fine grained
access control. Cells can have visibility labels, which is used to determine
whether a given user meets the security requirements to read the value.
This enables data of various security levels to be stored within the same
row, and users of varying degrees of access to query the same table,
while preserving data confidentiality.

Security labels consist of a set of user-defined tokens that are required
to read the value the label is associated with. The set of tokens required
can be specified using syntax that supports logical `AND` and `OR`
combinations of tokens, as well as nesting groups of tokens together.

JSON
====

Although Unicorn provides a modular and unified interface to various BigTable-like
systems, it is still a very low level API to manipulate data. A more productive way
to use Unicorn is through the rich, flexible, and easy-to-use document data model.
A document is essentially a JSON object with a unique key (corresponding to the row key).
With document data model, the application developers will focus on the
business logic while Unicorn efficiently maps documents to key-value pairs in BigTable.
In this section, we first introduce Unicorn's JSON data types and APIs.
In the next section, we will discuss the document API, which is compatible with MongoDB.

JSON (JavaScript Object Notation) is a lightweight data-interchange format.
It is easy for humans to read and write. It is easy for machines to parse
and generate. JSON is a text format that is completely language independent
but uses conventions that are familiar to programmers of the C-family of languages,

In Unicorn, it is very easy to parse a JSON object:

```scala
val doc =
  json"""
  {
    "store": {
      "book": [
        {
          "category": "reference",
          "author": "Nigel Rees",
          "title": "Sayings of the Century",
          "price": 8.95
        },
        {
          "category": "fiction",
          "author": "Evelyn Waugh",
          "title": "Sword of Honour",
          "price": 12.99
        },
        {
          "category": "fiction",
          "author": "Herman Melville",
          "title": "Moby Dick",
          "isbn": "0-553-21311-3",
          "price": 8.99
        },
        {
          "category": "fiction",
          "author": "J. R. R. Tolkien",
          "title": "The Lord of the Rings",
          "isbn": "0-395-19395-8",
          "price": 22.99
        }
      ],
      "bicycle": {
        "color": "red",
        "price": 19.95
      }
    }
  }
  """
```

The interpolator `json` parse a string to `JsObject`. It is also okay to embed
variable references directly in processed string literals.

```
  val x = 1
  json"""
    {
      "x": $x
    }
  """
```

If the string is not a JSON object but any other valid JSON expression, one may
use `parseJson` method to convert the string to `JsValue`.

```scala
  "1".parseJson
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