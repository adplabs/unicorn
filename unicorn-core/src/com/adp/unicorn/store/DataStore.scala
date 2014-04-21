/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store

/**
 * A NoSQL data store that include a number of data sets,
 * which don't have to support JOIN operation.
 * 
 * @author Haifeng Li (293050)
 */
trait DataStore {
    def dataset(name: String, auth: String = ""): DataSet
    def createDataSet(name: String): DataSet
    def createDataSet(name: String, stratey: String, replication: Int, columnFamilies: String*): DataSet
    def dropDataSet(name: String): Unit
}