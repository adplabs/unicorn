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
class CacheTest extends UnitSpec {
  trait MyContext extends Context {
    table.cacheOn
    "293050" of table
  }
  
  "Load from cache" should "pass" in new MyContext {
    "293050" of table
  }
}