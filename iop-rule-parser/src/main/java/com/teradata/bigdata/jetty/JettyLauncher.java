package com.teradata.bigdata.jetty;

import com.teradata.bigdata.jetty.servlet.HealthServlet;
import com.teradata.bigdata.jetty.servlet.HelloServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/2/002 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class JettyLauncher {

    public static void main(String[] args) throws Exception {

        // 指定端口号
        int port = 8080;
        Server server = new Server(port);
        WebAppContext context = new WebAppContext("webapp","/web");

        context.setDescriptor("webapp/WEB-INF/web.xml");

        String resourceBase = JettyLauncher.class
                .getClassLoader()
                .getResource("webapp")
                .toExternalForm();

        context.setResourceBase(resourceBase);
        // 项目名
        context.setDisplayName("IopRuleParser");
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        context.setConfigurationDiscovered(true);
        context.setParentLoaderPriority(true);
        context.addServlet(new ServletHolder(new HelloServlet()), "/hello");
        context.addServlet(new ServletHolder(new HealthServlet()), "/health");


        server.setHandler(context);
        System.out.println(context.getContextPath());
        System.out.println(context.getDescriptor());
        System.out.println(context.getResourceBase());
        System.out.println(context.getBaseResource());

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("server is  start, port is "+port+"............");
        Thread.sleep(1000);
        try{
            int a = 10/0;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            server.stop();
        }

    }
}
