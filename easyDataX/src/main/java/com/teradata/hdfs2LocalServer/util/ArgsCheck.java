package com.teradata.hdfs2LocalServer.util;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/8/29/029 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class ArgsCheck {

    private static HashMap<String, String[]> parseArgs(String[] args) {
        HashMap<String, String[]> tmp = new HashMap();
        ArrayList<String> arrValue = new ArrayList();
        String name = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                if (name != null) {
                    String[] argsValue = new String[arrValue.size()];
                    arrValue.toArray(argsValue);
                    tmp.put(name, argsValue);
                    arrValue.clear();
                }
                name = arg;
            } else {
                arrValue.add(arg);
            }
        }

        if (name != null) {
            String[] argsValue = new String[arrValue.size()];
            arrValue.toArray(argsValue);
            tmp.put(name, argsValue);
            arrValue.clear();
        }
        return tmp;
    }

    public static void main(String args[]) {
        //模拟命令行参数
        String cmdLine = "-arg1 1111 -arg3 2222 3333 -sum 1 2";
        args = cmdLine.split(" ");

        HashMap<String, String[]> parseArgs = parseArgs(args);
        for (String key : parseArgs.keySet()) {
            //打印参数名称
            System.out.println(key);

            String[] strings = parseArgs.get(key);
            //打印参数值
            for (String param : strings) {
                System.out.println("\t" + param);
            }
        }

        //使用示例
        if (parseArgs.containsKey("-sum")) {
            String[] vals = parseArgs.get("-sum");
            int i = 0;
            for (String item : vals) {
                i += Integer.parseInt(item);
            }
            System.out.println("SUM= " + i);
            return;
        }
    }
}
