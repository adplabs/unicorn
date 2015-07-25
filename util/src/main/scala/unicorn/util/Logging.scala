/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.util

import org.slf4j.LoggerFactory

/**
 * @author Haifeng Li (293050)
 */
trait Logging {

  lazy val log = LoggerFactory.getLogger(getClass)

}
