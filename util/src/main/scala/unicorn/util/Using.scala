/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.util

import java.io.Closeable

/**
 * @author Haifeng Li (293050)
 */
object Using {
  def apply[S <: Closeable, T](resource: S)(use: S => T): T = {
    try {
      use(resource)
    }
    finally {
      if (resource != null) resource.close
    }
  }
}
