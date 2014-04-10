package com.adp.cdg.store.accumulo

import com.adp.cdg._
import com.adp.cdg.DocumentImplicits._
import com.adp.cdg.store._

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