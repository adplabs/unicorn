package com.adp.cdg.store.accumulo

import com.adp.cdg._
import com.adp.cdg.DocumentImplicits._
import com.adp.cdg.store._

class CacheTest extends UnitSpec {
  trait MyContext extends Context {
    table.cacheOn
    "293050" of table
  }
  
  "Load from cache" should "pass" in new MyContext {
    "293050" of table
  }
}