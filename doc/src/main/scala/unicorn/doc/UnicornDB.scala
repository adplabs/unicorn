package unicorn.doc

/**
 * @author Haifeng Li
 */
class UnicornDB(db: unicorn.bigtable.Database) {
  val DefaultDocumentColumnFamily = "doc"

  /**
   * Returns a document collection.
   * @param name the name of collection/table.
   * @param family the column family that documents resident.
   */
  def apply(name: String, family: String = DefaultDocumentColumnFamily): DocumentCollection = {
    new DocumentCollection(db(name), family)
  }

  /**
   * Creates a document collection.
   * @param name the name of collection.
   * @param strategy the replica placement strategy.
   * @param replication the replication factor.
   * @param family the column family that documents resident.
   */
  def createCollection(name: String, strategy: String, replication: Int, family: String = DefaultDocumentColumnFamily): Unit = {
    db.createTable(name, strategy, replication, family)
  }

  /**
   * Drops a document collection. All column families in the table will be dropped.
   */
  def dropCollection(name: String): Unit = {
    db.dropTable(name)
  }
}
