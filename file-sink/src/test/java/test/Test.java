package test;

import java.text.SimpleDateFormat;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/5/005 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class Test {

    public static void main(String[] args) {
        long timeLong = System.currentTimeMillis();
        String date=new SimpleDateFormat("yyyyMMddHHmmss").format(timeLong);
        System.out.println(date);
    }
}
