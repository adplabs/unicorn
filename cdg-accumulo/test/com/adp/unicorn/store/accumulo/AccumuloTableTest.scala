/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo

import com.adp.unicorn._
import com.adp.unicorn.DocumentImplicits._
import com.adp.unicorn.store._

/**
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloSuite extends UnitSpec {
  "Load a non-existing document" should "pass" in new Context {
    "row1" of table
  }
  
  "Save a document into table" should "pass" in new Context {
    person into table
  }
  
  "Load it back from table" should "pass" in new Context {
    "293050" of table
  }  
  
  "Load partial document back from table" should "pass" in new Context {
    "293050".from(table).select("name", "gender")
  }  
}