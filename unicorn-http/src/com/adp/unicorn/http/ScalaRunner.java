/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.http;

import java.lang.management.ManagementFactory;

import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Java main entry to run the web service.
 * 
 * @author Haifeng Li (293050)
 */
public class ScalaRunner {
    public static void main(String[] args) throws Exception {
    	Server server = new Server(Integer.valueOf(System.getProperty("unicorn.http.port", "8080")));
        
    	// Setup JMX
        MBeanContainer mbContainer=new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        server.addBean(mbContainer);
        
        String webDir = ScalaRunner.class.getClassLoader().getResource("com/adp/unicorn/http/webapp").toExternalForm();
    	WebAppContext webapp = new WebAppContext();
    	webapp.setContextPath("/");
    	webapp.setResourceBase(webDir);
    	webapp.setDescriptor(webDir + "/WEB-INF/web.xml");
    	webapp.setParentLoaderPriority(true);

    	server.setHandler(webapp);
    	
	    server.start();
	    server.join();
    }
}