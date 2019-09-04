package test;

import com.xiaoleilu.hutool.http.HttpRequest;

import java.io.IOException;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/8/29/029 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class RestDemo {

    public static void main(String[] args) throws IOException {
//        String result2= HttpUtil.get("http://10.221.208.6:8088/ws/v1/cluster/apps?queue=root.bdoc.b_yz_app_td_yarn&state=RUNNING");
//
//        System.out.println(result2);

        String httpRequest = HttpRequest.get("http://10.221.208.6:8088/ws/v1/cluster/apps?queue=root.bdoc.b_yz_app_td_yarn&state=RUNNING&user.name=yarn").execute().body();;

        System.out.println(httpRequest);

    }
}
