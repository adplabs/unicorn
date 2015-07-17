/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo

/**
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloSuite extends UnitSpec {
  "Load a non-existing document" should "pass" in new Context {
    table.get("row1")
  }
  
  "Save a document into table" should "pass" in new Context {
    table.put(person)
  }
  
  "Load it back from table" should "pass" in new Context {
    table.get("293050")
  }  
  
  "Load partial document back from table" should "pass" in new Context {
    table.get("293050")
  }  
}