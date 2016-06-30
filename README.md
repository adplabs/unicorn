```
                        . . . .
                        ,`,`,`,`,
  . . . .               `\`\`\`\;
  `\`\`\`\`,            ~|;!;!;\!
   ~\;\;\;\|\          (--,!!!~`!       .
  (--,\\\===~\         (--,|||~`!     ./
   (--,\\\===~\         `,-,~,=,:. _,//
    (--,\\\==~`\        ~-=~-.---|\;/J,       Welcome to the Unicorn Database
     (--,\\\((```==.    ~'`~/       a |         BigTable, Document and Graph
       (-,.\\('('(`\\.  ~'=~|     \_.  \              Full Text Search
          (,--(,(,(,'\\. ~'=|       \\_;>
            (,-( ,(,(,;\\ ~=/        \                  Haifeng Li
            (,-/ (.(.(,;\\,/          )             ADP Innovation Lab
             (,--/,;,;,;,\\         ./------.
               (==,-;-'`;'         /_,----`. \
       ,.--_,__.-'                    `--.  ` \
      (='~-_,--/        ,       ,!,___--. \  \_)
     (-/~(     |         \   ,_-         | ) /_|
     (~/((\    )\._,      |-'         _,/ /
      \\))))  /   ./~.    |           \_\;
   ,__/////  /   /    )  /
    '===~'   |  |    (, <.
             / /       \. \
           _/ /          \_\
          /_!/            >_\

  Welcome to Unicorn Shell; enter ':help<RETURN>' for the list of commands.
  Type ":quit<RETURN>" to leave the Unicorn Shell
  Version 2.0.0, Scala 2.11.7, SBT 0.13.8, Built at 2016-05-23 04:21:17.922
===============================================================================

unicorn>
```

UNICORN
=======

[![Join the chat at https://gitter.im/haifengl/unicorn](https://badges.gitter.im/haifengl/unicorn.svg)](https://gitter.im/haifengl/unicorn?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Unicorn is a simple and flexible abstraction of BigTable-like database such as Cassandra,
HBase, Accumulo, and RocksDB. Beyond a unified interface to various database systems,
Unicorn provides easy-to-use document data model and MongoDB-like API.
Moreover, Unicorn supports directed property multigraphs and
documents can just be vertices in a graph.
<!---
Even more, full text search is provided with relevance ranking.
-->

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
A HTTP API, in the module `Rhino`, is also provided to non-Scala users.

For analytics, Unicorn data can be exported as RDDs in Spark.
These RDDs can also be converted to DataFrames or Datasets, which
support SQL queries. Unicorn graphs can be analyzed by Spark
GraphX too.

To use Unicorn as a library, add the following to SBT build file.

```scala
libraryDependencies += "com.github.haifengl" % "unicorn-unibase_2.11" % "2.0.0"
```

If you need additional HBase-only features, please link to the module `Narwhal`.

```scala
libraryDependencies += "com.github.haifengl" % "unicorn-narwhal_2.11" % "2.0.0"
```

With the module `Narwhal` that is specialized for HBase, advanced features such
as time travel, rollback, counters, server side filter, Spark integration, etc. are available.

To support the document model, Unicorn has a very rich and advanced JSON library.
With it, the users can operate JSON data just like in JavaScript. Moreover, it
supports JSONPath for flexibly analyse, transform and selectively extract data out
of JSON objects. Meanwhile, it is type
safe and may capture many errors during the compile time. To use only the JSON library,

```scala
libraryDependencies += "com.github.haifengl" % "unicorn-json_2.11" % "2.0.0"
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

If you want to work on the Unicorn code with IntelliJ, please first
[install SBT plugin](https://www.jetbrains.com/help/idea/2016.1/getting-started-with-sbt.html).
To import the project, on the main menu select
File | New | Project from Existing Sources.

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

You can also modify the configuration file `./conf/unicorn.ini` for
the memory and other JVM settings.

In the shell, type :help to print Scala interpreter help information.
To exit the shell, type :quit.

Connecting to Database
======================

Suppose that `hbase-site.xml` and `hbase-default.xml` can be found on the `CLASSPATH`,
one can connect to HBase as simple as

```scala
val db = HBase()
```

The user may also pass a Configuration object to `HBase()`
if HBase configuration files are not in the `CLASSPATH`.

To connect to Cassandra, please enables the Thrift API by configuring
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

RocksDB is an embeddable persistent key-value store for fast storage.
RocksDB builds on LevelDB to be scalable to run on servers with
many CPU cores, to efficiently use fast storage, to support IO-bound,
in-memory and write-once workloads.

If your data can be fit in one machine and low latency (in microseconds)
is important to your applications, RocksDB is a great choice. Especially
for graph database use cases, a graph traversal may touch thousands or
event millions vertices, the cost of IPC to a distributed database
will be too high. In this case, RocksDB will easily outperformance
other distributed storage engine.

There is no concept of tables in RocksDB. In fact, a RocksDB is like
a table in HBase. Therefore, we create a higher level concept of
database that contains multiple RocksDB databases in a directory.
Each RocksDB is actually a subdirectory, which is encapsulated
in RocksTable. To create RocksDB, simply provides a directory path.

```scala
val db = Unibase(RocksDB.create("/tmp/unicorn-twitter"))
```

To use an existing database,

```scala
val db = Unibase(RocksDB("/tmp/unicorn-twitter"))
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
definition time whereas new columns can be added
to any column family without pre-announcing them.
In contrast, column family are not static in Accumulo and can be
created on the fly. The only way to get a complete set of columns
that exist for a column family is to process all the rows.

A cell’s content is an uninterpreted array of bytes. And table
cells are versioned. A `(row, column, version)` tuple exactly specifies
a cell. The version is specified using a `long` integer. Typically this
`long` contains timestamps.

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

Setting `timestamp` as `0L`, `Put` creates a new version
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
is set, the "deleted" cells become effectively invisible for `Get`
operations but are not immediately removed from store files.
Note that there is a snag with the tombstone approach, namely
"deletes mask puts". Once a tombstone marker is set, even puts
after the delete will be masked by the delete tombstone.
Performing the put will not fail. However when you do a `Get`,
the `Put` has no effect but will start working after the major
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

JSON has only types of `string`, `number`, `boolean`, `object`, `array`, and `null`.
Unicorn includes additional types such as `date`, `int`, `long`, `double`, `counter`,
`binary`, `UUID`, `ObjectId` (as in BSON), etc.

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

```scala
val x = 1
json"""
  {
    "x": $x
  }
"""
```

If the string is not a JSON object but any other valid JSON expression, one may
use `parseJson` method to convert the string to a `JsValue`.

```scala
"1".parseJson
```

The `json` interpolator can only be applied to string literals. If you want to
parse a string variable, the `parseJson` method can always be employed. If you know the
string contains a JSON object, you may also use the method `parseJsObject`.

```scala
val s = """{"x":1}"""
s.parseJsObject
```

To serialize a JSON value (of type `JsValue`) in compact mode, you can just use `toString`.
To pretty print, use the method `prettyPrint`.

```scala
doc.toString
doc.prettyPrint
```

With a `JsObject` or `JsArray`, you can refer to the individual elements
with a variation of array syntax, like this:

```scala
doc("store")("bicycle")("color")
// Use symbol instead of string
doc('store)('bicycle)('color)
```

Note that we follow Scala's array access convention by `()` rather than
`[]` in JavaScript.

Besides, you can use the dot notation to access its fields/elements
just like in JavaScript:

```scala
doc.store.bicycle.color
doc.store.book(0).author
```

It is worth noting that we didn't define the type/schema of the document
while Scala is a strong type language. In other words, we have both
the type safe features of strong type language and the flexibility of dynamic
language in Unicorn's JSON library.

If you try to access a non-exist field, `JsUndefined` is returned.

```scala
unicorn> doc.book
res11: unicorn.json.JsValue = undefined
```

Although there are already several nice JSON libraries for Scala, the JSON
objects are immutable by design, which is a natural choice for a functional
language. However, Unicorn is designed for database, where data mutation is
necessary. Therefore, `JsObject` and `JsArray` are mutable data structures in
Unicorn. You can set/add a field just like in JavaScript:

```scala
json.store.bicycle.color = "green"
```

To delete a field from `JsObject`, use `remove` method:

```scala
doc.store.book(0) remove "price"
```

It is same as setting it `JsUndefined`:

```scala
doc.store.book(0).price = JsUndefined
```

To delete an element from `JsArray`, the `remove` method will effectively
remove it from the array. However, setting an element to `undefined` doesn't
reduce the array size.

```scala
// delete the first element and array size is smaller
doc.store.book.remove(0)
// set the first element to undefined but array size keeps same
doc.store.book(0) = JsUndefined
```

It is also possible to append an element or another array to `JsArray`:

```scala
val a = JsArray(1, 2, 3, 4)
a += 5

a ++= JsArray(5, 6)
```

Common iterative operations such as `foreach`, `map`, `reduce` can be applied to
`JsArray` too.

```scala
doc.store.book.asInstanceOf[JsArray].foreach { book =>
 println(book.price)
}
```

Because Scala is a static language, it is impossible to know
`doc.store.book` is an array at compile time. So it is typed
as generic `JsValue`, which is the parent type of specific
JSON data types. Therefore, we use `asInstanceOf[JsArray]`
to convert it to `JsArray` in order to use `foreach`.

With Unicorn, we can also look up field in the current object
and all descendants:

```scala
unicorn> doc \\ "price"
res29: unicorn.json.JsArray = [8.95,12.99,8.99,22.99,19.95]
```

For more advanced query operations, JSONPath can be employed.

JSONPath
--------

[JSONPath](http://goessner.net/articles/JsonPath/) is a means of
using XPath-like syntax to query JSON structures.
JSONPath expressions always refer to a JSON structure in the same way
as XPath expression are used in combination with an XML document.

```scala
val jspath = JsonPath(doc)
```

Since a JSON structure is usually anonymous and doesn't necessarily
have a "root member object" JSONPath assumes the abstract name
`$` assigned to the outer level object. Besides, `@` refers to the
current object/element.

```scala
// the authors of all books in the store
jspath("$.store.book[*].author")

// all authors
jspath("$..author")

// all things in store
jspath("$.store.*")

// the price of everything in the store
jspath("$.store..price")

// the third book
jspath("$..book[2]")

// the last book in order
jspath("$..book[-1:]")

// the first two books
jspath("$..book[0,1]")
jspath("$..book[:2]")

// filter all books with isbn number
jspath("$..book[?(@.isbn)]")

//filter all books cheaper than 10
jspath("$..book[?(@.price<10)]")

// all members of JSON structure
jspath("$..*")
```

Our JSONPath parser supports all queries except for queries
that rely on expressions of the underlying language
like `$..book[(@.length-1)]`. However, there’s usually
a ready workaround as you can execute the same query using `$..book[-1:]`.


Another deviation from JSONPath is to always flatten the results
of a recursive query. Using the bookstore example,
typically a query of `$..book` will return an array with one element,
the array of books. If there was another book
array somewhere in the document, then `$..book` will return an array
with two elements, both arrays of books.
However, if you were to query `$..book[2]` for our example,
you would get the second book in the first array,
which assumes that the `$..book` result has been flattened.
In Unicorn, we always flatten the result of recursive queries
regardless of the context.

It is also possible to update fields with JSONPath. Currently,
we support only child and array slice operators for update.

```scala
jspath("$['store']['book'][1:3]['price']") = 30.0
```

Document API
============

Unicorn's document APIs are defined in package `unicorn.unibase`,
which is independent of backend storage engine. Simply pass
a reference to storage engine to `Unibase()`, which provides the
interface of document model.

In relational database, a table is a set of tuples that have
the same attributes. In Unibase, a table contains documents, which
are JSON objects and may have nested objects and/or arrays.
Besides, the tables in Unibase do not enforce a schema. Documents
in a table may have different fields. Typically, all documents
in a collection are of similar or related purpose.
Moreover, documents in a table must have unique IDs/keys, which
is not necessary in relational databases.

In MongoDB, such a group of documents is called collection. In
order to avoid the confusion with Java/Scala's collection data
structures, Unibase simply calls it table.

```scala
val db = Unibase(Accumulo())
db.createTable("worker")
val workers = db("worker")
```

In above, we create a table `worker` in Unibase.
Then we create a JSON object and insert it into
the worker table as following. Note that we explicitly create
the `JsObject` by specifying the fields and values
instead of parsing from a JSON string. This way provides
fine controls on the data types of fields.

```scala
val joe = JsObject(
  "name" -> "Joe",
  "gender" -> "Male",
  "salary" -> 50000.0,
  "address" -> JsObject(
    "street" -> "1 ADP Blvd",
    "city" -> "Roseland",
    "state" -> "NJ",
    "zip" -> "07068"
  ),
  "project" -> JsArray("HCM", "NoSQL", "Analytics")
)

val key = workers.upsert(joe)
```

Each document should have a field `_id` as the primary key.
If it is missing, the `upsert` operation will generate a
random UUID as `_id`, which is returned and also added into
the input JSON object:

```scala
unicorn> joe.prettyPrint
res1: String =
{
  "address": {
    "city": "Roseland",
    "state": "NJ",
    "zip": "07068",
    "street": "1 ADP Blvd"
  },
  "name": "Joe",
  "gender": "Male",
  "salary": 50000.0,
  "project": ["HCM", "NoSQL", "Analytics"],
  "_id": "cc8a6a6d-4305-4b77-a776-6f70f2306d06"
}
```

If the input object includes `_id` and the table already
contains a document with same key, the document will be
overwritten by `upsert`. If this is not the preferred
behavior, one may use `insert`, which checks if the
document already exists and throws an exception
if so.

To make the code future proof, it is recommended to use
the constant value `$id`, defined in `Unibase` package object, instead of
`_id` in the code.

Besides UUID, one may also `Int`, `Long`, `Date`, `String`,
and BSON' `ObjectId` (12 bytes including 4 bytes timestamp,
3 bytes machine id, 2 bytes process id, and 3 bytes incrementer)
as the primary key. One may even use
a complex JSON data type such as object or array as the
primary key. However, this is NOT recommended because primary
keys cannot be updated once a document inserted. To achieve
similar effects, the old document has to be delete and
inserted again with new key. However, the old document
will be permanently deleted after the major compaction,
which may not be desired. Even before the document be
permanently deleted, the time travel functionality is
broken for this document.

To get the document back, simply treat the table as a
map and use `_id` as the key:

```scala
unicorn> workers(key).get.prettyPrint
res3: String =
{
  "address": {
    "city": "Roseland",
    "state": "NJ",
    "zip": "07068",
    "street": "1 ADP Blvd"
  },
  "name": "Joe",
  "gender": "Male",
  "salary": 50000.0,
  "project": ["HCM", "NoSQL", "Analytics"],
  "_id": "cc8a6a6d-4305-4b77-a776-6f70f2306d06"
}
```

If the document doesn't exist, `None` is returned.

To update a document, we use a MongoDB-like API:

```scala
val update = JsObject(
   "$id" -> key,
   "$set" -> JsObject(
     "salary" -> 100000.0,
     "address.street" -> "5 ADP Blvd"
   ),
   "$unset" -> JsObject(
     "gender" -> JsTrue
   )
)

workers.update(update)
```

The `$set` operator replaces the value of a field with
the specified value, provided that the new field does not
violate a type constraint (and the document key `_id`
should not be set). If the field does not exist, `$set`
will add a new field with the specified value.
To specify a field in an embedded object or in an array,
use dot notation. To be compatible to MongoDB, we concatenate
the array name with the dot (.) and zero-based index position
to specify an element of an array by the zero-based index position,
which is different from the `[]` convention in JavaScript and JSONPath.

In MongoDB, `$set` will create the embedded objects as needed to fulfill
the dotted path to the field. For example, for a `$set {"a.b.c" : "abc"}`,
MongoDB will create the embedded object "a.b" if it doesn't exist.
However, we don't support this behavior because of the performance considerations.
We suggest the the alternative syntax ``{"a.b" : {"c" : "abc"}}``, which has the
equivalent effect.

If you want to use `json` string interpolation to create a
JSON object for `update`, remember to escape `$set` and `$unset` by double
dollar sign, e.g. `$$set` and `$$unset`.

To delete a document, use the method `delete` with the document key:

```scala
workers.delete(key)
```

Append Only
-----------

When creating a table, we may declare it append only.
Such a table is write-once (i.e. the same document is
never updated). Any updates
to existing documents will throw exceptions. Deletes are
not allowed either.

```scala
db.createTable("stock", appendOnly = true)
val prices = db("stock")
val trade = json"""
  {
    "ticker": "GOOG",
    "price": 700.0,
    "timestamp": ${System.currentTimeMillis}
  }
"""
val key = prices.upsert(trade)
prices.update(JsObject(
  "_id" -> key,
  "$set" -> JsObject(
    "price" -> JsDouble(800.0)
  )
))


java.lang.UnsupportedOperationException
  at unicorn.unibase.Table.update(Table.scala:320)
  ... 52 elided
```

Multi-Tenancy
-------------

Multi-tenant tables are regular tables that
enables views to be created over the table
across different tenants.
This option is useful to share the same physical BigTable
table across many different tenants.

To use a multi-tenant table, the user must firstly
set the tenant id, which cannot be `undefined`, `null`,
`boolean`, `counter`, `date`, or `double`.
The tenats only see their data in such tables.

```scala
val workers = db("worker")
workers.tenant = "IBM"
val ibmer = workers.upsert(json"""
  {
    "name": "Tom",
    "age": 40
  }
""")

workers.tenant = "Google"
val googler = workers.upsert(json"""
  {
    "name": "Tom",
    "age": 30
  }
""")
```

Because the tenant is "Google" now, the data of tenant "IBM"
are not visible.

```scala
unicorn> workers(ibmer)
res5: Option[unicorn.json.JsObject] = None
unicorn> workers(googler)
res6: Option[unicorn.json.JsObject] = Some({"name":"Tom","age":30,"_id":"545ed4d1-280c-4b6a-a3cc-e0a3c5fc5b43"})
```

Switch back to "IBM", the view is different:

```scala
unicorn> workers.tenant = "IBM"
workers.tenant: Option[unicorn.json.JsValue] = Some(IBM)
unicorn> workers(ibmer)
res8: Option[unicorn.json.JsObject] = Some({"name":"Tom","age":40,"_id":"2b7fb69f-810f-4ca7-a70f-5db767bc8e49"})
unicorn> workers(googler)
res9: Option[unicorn.json.JsObject] = None
```

As a client-side solution, Unicorn does not enforce security on
multi-tenant tables. In fact, there are no concepts of user and/or role.
It is the application's responsibility to ensure the authorization
and the authentication
on the access of multi-tenant tables.

Locality
--------

When Unicorn creates a document table, it creates only one column
family by default.

```scala
db.createTable("worker",
  families = Seq(Unibase.DocumentColumnFamily),
  locality = Map().withDefaultValue(Unibase.DocumentColumnFamily))
```

where the parameter `families` is the list of column families,
and the parameter `locality`, a map, tells Unicorn how to
map the data to different column families. Because we have
only one column families here, we simply set the default
value of map is the only column family.

This schema can be customized. When documents in a table have a lot of fields
and only a few fields are needed in many situations,
it is a good idea to organize them into different
column families based on business logic and access patterns.
Such a design is more efficient because the storage engine needs scan only
the necessary column family. It also limits the network data transmission.

```scala
db.createTable("worker",
  families = Seq(
    "id",
    "person",
    "address",
    "project"),
  locality = Map(
    $id -> "id",
    "address" -> "address",
    "project" -> "project"
  ).withDefaultValue("person"))
```

For simplicity, Unicorn uses only the top level
fields of documents to determine the locality mapping.

```scala
val joe = JsObject(
  "name" -> "Joe",
  "gender" -> "Male",
  "salary" -> 50000.0,
  "address" -> JsObject(
    "street" -> "1 ADP Blvd",
    "city" -> "Roseland",
    "state" -> "NJ",
    "zip" -> "07068"
  ),
  "project" -> JsArray("HCM", "NoSQL", "Analytics")
)

val key = workers.upsert(joe)
```

We can retrieve partial documents as following, which
is known as "projection" in relational database and MongoDB.

```scala
unicorn> workers(key)
res17: Option[unicorn.json.JsObject] = Some({"address":{"city":"Roseland","state":"NJ","zip":"07068","street":"1 ADP Blvd"},"project":["HCM","NoSQL","Analytics"],"_id":"6df63cf3-e4dd-4381-8276-ac0c0626dc78"})
unicorn> workers(key, "address")
res19: Option[unicorn.json.JsObject] = Some({"address":{"city":"Roseland","state":"NJ","zip":"07068","street":"1 ADP Blvd"},"_id":"6df63cf3-e4dd-4381-8276-ac0c0626dc78"})
unicorn> workers(key, "project")
res20: Option[unicorn.json.JsObject] = Some({"project":["HCM","NoSQL","Analytics"],"_id":"6df63cf3-e4dd-4381-8276-ac0c0626dc78"})
```

You can retrieve data from multiple column families too.

```scala
unicorn> workers(key, "name", "address")
res21: Option[unicorn.json.JsObject] = Some({"address":{"city":"Roseland","state":"NJ","zip":"07068","street":"1 ADP Blvd"},"name":"Joe","gender":"Male","salary":50000.0,"_id":"6df63cf3-e4dd-4381-8276-ac0c0626dc78"})
```

However, there is a semantic difference from regular projection
as you may notice in the above. Even though the user asks
for only `name`, all other fields in the same column family
are returned. This is due to the design of BigTable and the mapping
from document fields to columns.
For example, if a specified
field is a nested object, there is no easy way to read only the specified object in BigTable.
Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
nested objects in request, we have to send multiple `Get` requests, which is not efficient. Instead,
we return the whole object of a column family if some of its fields are in request. This is usually
good enough for hot-cold data scenario. For instance of a table of events, each event has a
header in a column family and event body in another column family. In many reads, we only need to
access the header (the hot data). When only user is interested in the event details, we go to read
the event body (the cold data). Such a design is simple and efficient. Another difference from MongoDB is
that we do not support the `excluded` fields.

Script
------

We may also run Unicorn code as a shell script or batch command.
The following bash script can be run directly from the command shell:

```scala
#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.accumulo._
import unicorn.bigtable.hbase._
import unicorn.unibase._

val db = Unibase(Accumulo())
db.createTable("worker")
val workers = db("worker")

val joe = JsObject(
  "name" -> "Joe",
  "gender" -> "Male",
  "salary" -> 50000.0,
  "address" -> JsObject(
    "street" -> "1 ADP Blvd",
    "city" -> "Roseland",
    "state" -> "NJ",
    "zip" -> "07068"
  ),
  "project" -> JsArray("HCM", "NoSQL", "Analytics")
)

workers.upsert(joe)
```

Narwhal
=======

Advanced document API with HBase is available in the package
`unicorn.narwhal`. To use these features, the user should
use the class `Narwhal` and `HTable`, which are subclasses
of `Unibase` and `Table`, respectively.

```scala
val db = new Narwhal(HBase())
db.createTable("narwhal")

val rich = json"""
  {
    "owner": "Rich",
    "phone": "123-456-7890",
    "address": {
      "street": "1 ADP Blvd.",
      "city": "Roseland",
      "state": "NJ"
    },
    "children": 2C
  }
"""

val bucket = db("narwhal")
val key = bucket.upsert(rich)
```

Counter
-------

The usage of `Narwhal` and `HTable` are similar to their
parent classes. Besides, additional operators are available
by taking advantage of HBase's features. Note that in the
above the field `children` takes the value of `2C`. The suffix
`C` indicates it is a counter. You may use `JsCounter` to create
a counter directly too. With counters, we may update them
in an atomic operation. For a regular integer, we instead have
to read, update, and write back, which may cause consistency
problems.

```scala
val increment = json"""
  {
    "$$inc": {
      "children": 1
    }
  }
"""

increment("_id") = key
bucket.update(increment)

val doc = bucket(key).get
println(doc.children)
```

This example will print out the new value, i.e. 3, of `children`.
It is also possible to use a negative value in `$inc` operations, which
effectively decreases the counter.

Rollback
--------

Another interesting feature is that we can rollback document fields
back to previous values. If a user accidentally changes a value, don't
worry. The old values are still in HBase and we can easily rollback.

```scala
val update = json"""
  {
    "$$set": {
      "phone": "212-456-7890",
      "gender": "M",
      "address.street": "5 ADP Blvd."
    }
  }
"""

update("_id") = key
bucket.update(update)
```

In the above example, we update three fields `phone`, `gender`,
and `address.street`. Note that `gender` is actually a new filed.
Let's verify these fields be updated.

```
unicorn> bucket(key).get
res11: unicorn.json.JsObject = {"children":2,"address":{"city":"Roseland","state":"NJ","street":"5 ADP Blvd."},"owner":"Rich","gender":"M","_id":"0c354c07-6e25-4b30-9cf3-9a508b6868fd","phone":"212-456-7890"}
```

Now we rollback `phone` and `gender`.

```scala
val rollback = json"""
  {
    "$$rollback": {
      "phone": 1,
      "gender": 1
    }
 }
"""
rollback("_id") = key
bucket.update(rollback)
```

Printing out the document, we can find that `phone` has the previous value and `gender` disappears now.
Of course, `address.street` still keeps the latest values.

```
unicorn> bucket(key).get
res15: unicorn.json.JsObject = {"children":4,"address":{"city":"Roseland","state":"NJ","street":"5 ADP Blvd."},"owner":"Rich","_id":"0c354c07-6e25-4b30-9cf3-9a508b6868fd","phone":"123-456-7890"}
```

This example shows how we can rollback `$set` operations. We actually
can also rollback `$unset` operations.

```scala
val update = json"""
  {
    "$$unset": {
      "owner": 1,
      "address": 1
    }
  }
"""

update("_id") = key
bucket.update(update)

val rollback = json"""
  {
    "$$rollback": {
      "owner": 1,
      "address": 1
    }
  }
"""

rollback("_id") = key
bucket.update(rollback)
```

Time Travel
-----------

Another cool feature of Narwhal is time travel. Because HBase stores multiple timestamped values,
we can query the snapshot of a document at a given time point.

```scala
val asOfDate = new Date

val update = json"""
  {
    "$$set": {
      "phone": "212-456-7890",
      "gender": "M",
      "address.street": "5 ADP Blvd."
    },
    "$$inc": {
      "children": 1
    }
  }
"""

update("_id") = key
bucket.update(update)

bucket(asOfDate, key)
```

Besides the plain `Get`, we can supply an as-of-date parameter in a time travel `Get`,
which will retrieval document's value at that time point.

Filter
------

So far, we get documents by their keys. With HBase, we can also
query documents with method `find` by filtering field values.
The `find` method returns an iterator to the documents that match the query criteria.

```scala
bucket.upsert(json"""{"name":"Tom","age":30,"state":"NY"}""")
bucket.upsert(json"""{"name":"Mike","age":40,"state":"NJ"}""")
bucket.upsert(json"""{"name":"Chris","age":30,"state":"NJ"}""")

val it = bucket.find(json"""{"name": "Tom"}""")
it.foreach(println(_))
```

Optionally, the `find` method takes the second parameter
for projection, which is an object that specifies the fields to return.

The `find` method with no parameters returns all documents
from a table and returns all fields for the documents.

The syntax of filter object is similar to MongoDB.
Supported operators include `$and`, `$or`, `$eq`, `$ne`,
`$gt`, `$gte` (or `$ge`), `$lt`, `$lte` (or `$le`).

```scala
bucket.find(json"""
  {
    "$$or": [
      {
        "age": {"$$gt": 30}
      },
      {
        "state": "NY"
      }
    ]
  }
""")
```

If you prefer SQL-like query, just do it as follows:

```scala
bucket.find("""age > 30 OR stage = "NY"""")
```

Note that the query operation is based on server side filters.
Although it minimizes the network transmission, it is still
a costly full table scan. If the table is multi-tenanted and each tenant
does not have too much data (e.g. SaaS solutions for small business),
however, the scan will be usually localized
to one or a few nodes and often quite fast. Compared to secondary index,
this approach does not have penalty on the write performance and
still provides fairly good performance on queries in such a situation.

For general purpose queries, secondary index should be built
to accelerate frequent queries. We will discuss our secondary index
design in the below.

SQL
---

In fact, you can do a SQL-like query in the code or shell, which
returns a `DataFrame`.

```scala
db.sql("""SELECT address.state,
            COUNT(address.state),
            MAX(age),
            AVG(salary) as avg_salary
          FROM worker
          GROUP BY address.state
          ORDER By avg_salary""")
```

Although filtering is done on the server side, all other heavy
computation such as `SUM`, `GROUP BY`, `ORDER BY`, etc. are done
on the client side. So this is only useful for small operational
queries that involves a handful rows. For large scale analytics
queries, we should use Spark as discussed below.

Spark
-----

For large scale analytics, Narwhal supports Spark. A table can be exported
to Spark as `RDD[JsObject]`.

```scala
import org.apache.spark._
import org.apache.spark.rdd.RDD

val conf = new SparkConf().setAppName("unicorn").setMaster("local[4]")
val sc = new SparkContext(conf)

val db = new Narwhal(HBase())
val table = db("worker")
table.tenant = "IBM"
val rdd = table.rdd(sc)
rdd.count()
```

In the above example, we first create a `SparkContext`. To export a table
to spark, simply pass the `SparkContext` object to the `rdd` method of a
table object. In this example, we only export the data of tenant `IBM`.

Although Spark has filter functions on `RDDs`, it is better to use
HBase's server side filter at beginning to reduce network transmission.
The `rdd` method can take the second optional parameter for filtering,
same syntax as in `find`.

```scala
val table = db("narwhal")
val rdd = table.rdd(sc, json"""
                          {
                            "$$or": [
                              {
                                "age": {"$$gt": 30}
                              },
                              {
                                "state": "NJ"
                              }
                            ]
                          }
                        """)
rdd.count()
```

For analytics, `SQL` is still the best language. We can easily
convert `RDD[JsObject]` to a strong-typed `DataFrame` to be
analyzed in `SparkSQL`.

```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

case class Worker(name: String, age: Int)
val workers = rdd.map { js => Worker(js.name, js.age) }
val df = sqlContext.createDataFrame(workers)
df.cache
df.show

df.registerTempTable("worker")
sqlContext.sql("SELECT * FROM worker WHERE age > 30").show
```

In the above example, we create a `SQLContext` on top of `SparkContext`
to use SparkSQL features. Then we create a `DataFrame` of case class
`Worker` with type information. Since we will use this `DataFrame`
many times in analysis, we also `cache` it with an in-memory columnar format.
To do SQL queries, we also register this `DataFrame` as a temporary table.
<!--
Note that in Spark 2.0, `DataFrame` will be just a special case of
`Dataset` (`Dataset[Row]`). If you are using Spark 2.0, the above
example will be slightly adjusted.
-->

Graph
=====

Graphs are mathematical structures used to model pairwise relations
between objects. A graph is made up of vertices (nodes) which are
connected by edges (lines). A graph may be undirected,
meaning that there is no distinction between the two vertices
associated with each edge, or its edges may be directed from
one vertex to another. Directed graphs are also called digraphs
and directed edges are also called arcs or arrows.

A multigraph is a graph which is permitted to have multiple
edges (also called parallel edges), that is, edges that have
the same end nodes.
The ability to support parallel edges simplifies modeling
scenarios where there can be multiple relationships
(e.g., co-worker and friend) between the same vertices.

In a property graph, the generic mathematical graph is often extended
to support user defined objects attached to each vertex and edge.
The edges also have associated labels denoting the relationships,
which are important in a multigraph.

Unicorn supports directed property multigraphs. Documents from different
tables can be added as vertices to a multigraph. It is also okay to add
vertices without corresponding to documents. Each relationship/edge
has a label and optional data (any valid JsValue, default value JsInt(1)).

Unicorn stores graphs in adjacency lists. That is, a graph
is stored as a BigTable whose rows are vertices with their adjacency list.
The adjacency list of a vertex contains all of the vertex’s incident edges
(incoming and outgoing edges are in different column families).

Because large graphs are usually very sparse, an adjacency list is
significantly more space-efficient than an adjacency matrix.
Besides, the neighbors of each vertex may be listed efficiently with
an adjacency list, which is important in graph traversals.
With our design, it is also possible to
test whether two vertices are adjacent to each other
for a given relationship in constant time.

In what follows, we create a graph of gods, an example
from Titan graph database.

```scala
val db = Unibase(Accumulo())
db.createGraph("gods")
val gods = db.graph("gods", new Snowflake(0))
```

Because each vertex in the graph must have a unique
64-bit ID, we should provide a (distributed) ID generator
when we go to mutate a graph. We currently
provide the Snowflake ID generator, designed by Twitter.
Each Snowflake worker should have a unique worker ID
so that multiple worker won't generate duplicate
IDs in parallel. In a small system, these worker IDs
may be hand picked. For a large system, it is better
coordinated by ZooKeeper. In this demo, we simply
use `0` as the worker ID in the shell. For production,
you may do things like

```scala
db.graph("gods", "zookeeper connection string")
```

If no ID generator is provided, `db.graph("gods")` returns
a read only instance for graph traversal or analytics.

In the next, we will add several vertices with properties
stored in a `JsObject`.
The function `addVertex` returns the ID of type `Long` of new vertex.

```scala
val saturn = gods.addVertex(json"""{"label": "titan", "name": "saturn", "age": 10000}""")
val sky = gods.addVertex(json"""{"label": "location", "name": "sky"}""")
val sea = gods.addVertex(json"""{"label": "location", "name": "sea"}""")
val jupiter = gods.addVertex(json"""{"label": "god", "name": "jupiter", "age": 5000}""")
val neptune = gods.addVertex(json"""{"label": "god", "name": "neptune", "age": 4500}""")
val hercules = gods.addVertex(json"""{"label": "demigod", "name": "hercules", "age": 30}""")
val alcmene = gods.addVertex(json"""{"label": "human", "name": "alcmene", "age": 45}""")
val pluto = gods.addVertex(json"""{"label": "god", "name": "pluto", "age": 4000}""")
val nemean = gods.addVertex(json"""{"label": "monster", "name": "nemean"}""")
val hydra = gods.addVertex(json"""{"label": "monster", "name": "hydra"}""")
val cerberus = gods.addVertex(json"""{"label": "monster", "name": "cerberus"}""")
val tartarus = gods.addVertex(json"""{"label": "location", "name": "tartarus"}""")
```

Of course, we will also add edges between vertices.
It is important that edges have direction. With
`addEdge(jupiter, "father", saturn)`, we add an edge
from `jupiter` to `saturn` with label `father`.
For this edge, `jupiter` is called out vertex while
`saturn` is in vertex. From the point view of vertex,
this edge is an outgoing edge of `jupiter` while
the incoming edge of `saturn`.

Besides, we may also associate any `JsValue` to an edge.
If no value is provided, the default value is `1`.

```scala
gods.addEdge(jupiter, "father", saturn)
gods.addEdge(jupiter, "lives", sky, json"""{"reason": "loves fresh breezes"}""")
gods.addEdge(jupiter, "brother", neptune)
gods.addEdge(jupiter, "brother", pluto)

gods.addEdge(neptune, "lives", sea, json"""{"reason": "loves waves"}""")
gods.addEdge(neptune, "brother", jupiter)
gods.addEdge(neptune, "brother", pluto)

gods.addEdge(hercules, "father", jupiter)
gods.addEdge(hercules, "mother", alcmene)
gods.addEdge(hercules, "battled", nemean, json"""{"time": 1, "place": {"latitude": 38.1, "longitude": 23.7}}""")
gods.addEdge(hercules, "battled", hydra, json"""{"time": 2, "place": {"latitude": 37.7, "longitude": 23.9}}""")
gods.addEdge(hercules, "battled", cerberus, json"""{"time": 12, "place": {"latitude": 39.0, "longitude": 22.0}}""")

gods.addEdge(pluto, "brother", jupiter)
gods.addEdge(pluto, "brother", neptune)
gods.addEdge(pluto, "lives", tartarus, json"""{"reason": "no fear of death"}""")
gods.addEdge(pluto, "pet", cerberus)

gods.addEdge(cerberus, "lives", tartarus)
```

Correspondingly, `deleteEdge` removes an edge and
`deleteVertex` removes the vertex and all associated edges.

To retrieve a vertex and its edge, `gods(jupiter)` will return
a `Vertex` object containing its ID, properties, and associated
edges. One can also directly access the data of an edge by
`gods(pluto, "brother", jupiter)`. If the edge doesn't exist,
`None` is returned.

Although some graphs (e.g. Twitter graph) natively use `Long` integer as
vertex IDS, many graphs use Strings as vertex IDs. In RDF
(Resource Description Framework), for example, vertices are encoded
as URI. In Unicorn, we can use strings as vertex keys too.
Internally, Unicorn translates them to the corresponding `Long` vertex
IDs. However, it is recommended to use `Long` vertex ID directly if
possible because it provides better performance.

It is also possible to add a document (in another table) as a vertex
to a graph.

```scala
// key is the document key in the table "person"
val id = gods.addVertex("person", key)
```

If the `person` table is multi-tenanted, remember to use tenant id
as the third argument. To access the vertex, it is simply as
`gods("person", key)`.

Gremlin-like API
----------------

For graph traversal, we support a [Gremlin](http://tinkerpop.apache.org)-like API. To start a
traversal,

```scala
val g = gods.traversal
```

Then we can start with one or more vertex by the method `v`,

```scala
g.v(saturn)
```

A Traversal is essentially an Iterator of vertices or edges.
On a vertex, we can call `outE()` and `inE()` to access its
outgoing edges or incoming edges, respectively. On an
edge, `inV()` and `outV()` returns its in vertex and out vertex,
respectively. For vertex, `out()` is a shortcut to `outE().inV()`
and similarly `in()` is shortcut to `inE().outV()`. All these
functions may take a set of labels to filter relationships.

The following example shows how to get saturn's grandchildren's name.

```scala
g.v(saturn).in("father").in("father").name
```
For detailed information on Gremlin, please refer its [website](http://tinkerpop.apache.org).

Graph Search
------------

Beyond simple graph traversal, Unicorn supports DFS, BFS, A* search,
Dijkstra algorithm, etc.

The below example searches for the shortest path between jupiter
and cerberus with Dijkstra algorithm.

```scala
val path = GraphOps.dijkstra(jupiter, cerberus, new SimpleTraveler(gods)).map { edge =>
  (edge.from, edge.label, edge.to)
}

path.foreach(println(_))
```

Note that this search is performed by a single machine. For very
large graph, it is better to use some distributed graph computing engine
such as Spark GraphX.

Spark GraphX
------------

With HBase/Narwhal, we can export a graph to Spark GraphX
for advanced analytics such as PageRank, triangle count, SVD++, etc.

```scala
import org.apache.spark._

val conf = new SparkConf().setAppName("unicorn").setMaster("local[4]")
val sc = new SparkContext(conf)

val graph = db.graph("gods")
val graphx = graph.graphx(sc)

// Run PageRank
val ranks = graphx.pageRank(0.0001).vertices
```

HTTP API
========

So far we access Unicorn through its Scala APIs. For other programming
language users, we can manipulate documents with the HTTP API, which
is provided by the Rhino module.

In the configuration file `conf/rhino.conf`, the underlying BigTable database
engine should be configured in the section `uncorn.rhino`. The configuration
file is in the format of [Typesafe Config](https://github.com/typesafehub/config).
For example,

```
unicorn.rhino {
  bigtable = "hbase"
  accumulo {
    instance = "local-poc"
    zookeeper = "127.0.0.1:2181"
    user = "root"
    password = "secret"
  }
  cassandra {
    host = "127.0.0.1"
    port = 9160
  }
}
```

In this example, we use HBase as the BigTable engine. Note that the configuration
of HBase is in its own configuration files, which should be in the `CLASSPATH` of
Rhino. Sample configurations of Accumulo and Cassandra are provided in the
example for demonstration.

Currently, Rhino provides only data manipulation operations.
Other operations such as table creation/drop should be done in the
Unicorn Shell.

| Method   | URL                       | Operation |
| -------- | ------------------------- | --------- |
| PUT      | /table/[table name]       | Insert    |
| POST     | /table/[table name]       | Upsert    |
| PATCH    | /table/[table name]       | Update    |
| DELETE   | /table/[table name]       | Delete    |
| DELETE   | /table/[table name]/key   | Delete    |
| GET      | /table/[table name]       | Get       |
| GET      | /table/[table name]/key   | Get       |

The API is simple and easy to use. To insert a document,
use the `PUT` method with the JSON object as entity-body.

```bash
curl -X PUT -H "Content-Type: application/json" -d '{"_id":"dude","username":"xyz","password":"xyz"}' http://localhost:8080/table/rhino_test_table
```

To read it back, simply use `GET` method. If the key is a string,
the key can be part of the URI. Otherwise, the key should be
set in the entity-body. The same rule applies to the `DELETE` method.

```bash
curl -X GET http://localhost:8080/table/unicorn_rhino_test/dude
```

To update a document, use the `PATCH` method.

```bash
curl -X PATCH -H "Content-Type: application/json" -d '{"_id":"dude","$set":{"password":"abc"}}' http://localhost:8080/table/rhino_test_table
```

In case of multi-tenancy, the tenant id should be set in
the header.

```bash
curl -X PUT -H "Content-Type: application/json" --header 'tenant: "IBM"' -d '{"_id":"dude","username":"xyz","password":"xyz"}' http://localhost:8080/table/rhino_test_table

curl -X GET --header 'tenant: "IBM"' http://localhost:8080/table/rhino_test_table/dude

curl -X GET --header 'tenant: "MSFT"' http://localhost:8080/table/rhino_test_table/dude

curl -X DELETE --header 'tenant: "IBM"' http://localhost:8080/table/rhino_test_table/dude
```

