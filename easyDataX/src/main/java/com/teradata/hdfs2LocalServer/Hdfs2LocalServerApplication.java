package com.teradata.hdfs2LocalServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.Scanner;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/8/29/029 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
@SpringBootApplication
public class Hdfs2LocalServerApplication {

    public static void main(String[] args) {
//        SpringApplication.run(Hdfs2LocalServerApplication.class,args);
        System.out.println("Please enter the server startup port number:");
        Scanner scan = new Scanner(System.in);
        String port = scan.nextLine();
        new SpringApplicationBuilder(Hdfs2LocalServerApplication.class).properties("server.port=" + port).run(args);
    }
}
