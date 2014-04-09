package com.adp.cdg.store

import com.adp.cdg.Document

/**
 * A NoSQL data store that include a number of data sets,
 * which don't have to support JOIN operation.
 */
trait DataStore {
    def dataset(name: String, auth: String = "public"): DataSet
    def createDataSet(name: String, columnFamilies: String*): DataSet
    def dropDataSet(name: String): Unit
}