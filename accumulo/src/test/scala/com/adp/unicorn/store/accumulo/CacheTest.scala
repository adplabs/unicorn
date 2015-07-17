/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo

/**
 * 
 * @author Haifeng Li (293050)
 */
class CacheTest extends UnitSpec {
  trait MyContext extends Context {
    table.cacheOn
    table.get("293050")
  }
  
  "Load from cache" should "pass" in new MyContext {
    table.get("293050")
  }
}