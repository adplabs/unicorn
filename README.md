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
  Version 2.0.0, Scala 2.11.7, SBT 0.13.8, Built at 2016-04-18 15:02:18.234
===============================================================================

unicorn>
```

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

To use only JSON library,

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

Suppose that `hbase-site.xml` and `hbase-default.xml` can be found on the `CLASSPATH`,
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
use `parseJson` method to convert the string to `JsValue`.

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
jspath("$['store']['book'][1:3]['price']") = 30
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
   "_id" -> key,
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

```scala
db.createTable("worker", multiTenant = true)
```

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
multi-tenant tables. In fact, there is no user or role concepts.
It is the application's responsibility to ensure the authorization
and the authentication
on the access of multi-tenant tables.

Locality
--------

When Unicorn creates a document table, it creates multiple column
families. By default, one column family for document id,
the second for document fields, and the third for graph data
(to be discussed in the next section).
Such a design is efficient when you need only either the document
data or the graph data because the storage engine needs scan only
the necessary column family. It also limits the network data transmission.

This schema can be customized. For example, if you do not have
graph data, you may want to use only one column family
for both document id and data.

```scala
db.createTable("worker",
  families = Seq(Unibase.DefaultDocumentColumnFamily),
  locality = Map().withDefaultValue(Unibase.DefaultDocumentColumnFamily))
```

where the parameter `families` is the list of column families,
and the parameter `locality`, a map, tells Unicorn how to
map the data to different column families. Because we have
only one column families here, we simply set the default
value of map is the only column family.

When documents in a table have a lot of fields
and only a few fields are needed in in many situations,
it is a good idea to organize them into different
column families based on business logic and access patterns.

```scala
db.createTable("worker",
  families = Seq(
    Unibase.DefaultIdColumnFamily,
    "address",
    "project"),
  locality = Map(
    Unibase.$id -> Unibase.DefaultIdColumnFamily,
    "address" -> "address",
    "project" -> "project"
  ).withDefaultValue(Unibase.DefaultDocumentColumnFamily))
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

we can retrieve partial documents as following, which
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
are returned. This is due to the design of BigTable.
For example, if a specified
field is a nested object, there is no easy way to read only the specified object in BigTable.
Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
nested objects in request, we have to send multiple Get requests, which is not efficient. Instead,
we return the whole object of a column family if some of its fields are in request. This is usually
good enough for hot-cold data scenario. For instance of a table of events, each event has a
header in a column family and event body in another column family. In many reads, we only need to
access the header (the hot data). When only user is interested in the event details, we go to read
the event body (the cold data). Such a design is simple and efficient. Another difference from MongoDB is
that we do not support the excluded fields.

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
It is also okey to use a negative value in `$inc` operations, which
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

Besides the plain `Get`, we can supply an as-of-date in a time travel `Get`,
which will retrieval document's value at that time point.

Filter
------

So far, we get documents by their keys. With HBase, we can also
query documents with method `find` by filtering field values.
The `find` method returns an iterator to the documents that match the query criteria.

```scala
bucket.upsert(json"""{"name":"Tom","age":30,"home_based":true}""")
bucket.upsert(json"""{"name":"Mike","age":40,"home_based":false}""")
bucket.upsert(json"""{"name":"Chris","age":30,"home_based":false}""")

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
        "home_based": false
      }
    ]
  }
""")
```

Rhino
=====

Graph
=====

Beyond documents, Unicorn supports the graph data model.
In Unicorn, documents are (optionally) vertices/nodes
in a graph, which is permitted to have multiple parallel
edges/relationships, that is, edges that have the same
end nodes as long as different tags. Each relationship/edge
is defined as a tuple (source, target, tag, value), where
source and target are documents, tag is the label of relationship,
and value is any kind of JSON value associated with the relationship.


```scala
val tom = json"""
  {
    "name": "Tom"
  }
"""

workers.upsert(tom)


graph(joe)("works with", tom._id) = 1

// Query relationships to Jim
graph(joe)(tome._id)

// Query neighbors of given relationship(s)
graph(joe)("works with")

// Gets the value/object associated with the given relationship.
// If it doesn't exist, undefined will be returned.
graph(joe)("works with", tom._id)
```

It is easy to query the neighbors or relationships as shown
in the example. However, it is more interesting to do
graph traversal, shortest path and other graph analysis.
Unicorn supports DFS, BFS, A* search, Dijkstra algorithm,
topological sort and more with user defined functions.
The below code snippet shows how to use A* search to find
the shortest path between two nodes.

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

In this example, the graph processing is performed on
the database directly. This is fine because most nodes
will be accessed only once in A* search. However, it will
be costly for many advanced graph algorithms that may access
nodes or edges multiple times. Unicorn provides DFS and BFS
to create in-memory sub-graphs stored in DocumentGraph objects
that is more suitable for things like network flow, assignment, coloring, etc.


