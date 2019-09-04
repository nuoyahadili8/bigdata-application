package com.teradata.hdfs2LocalServer.controller;

import com.teradata.hdfs2LocalServer.util.R;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/8/29/029 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
@RestController("/health")
public class HealthController {

    @RequestMapping("/state")
    @ResponseBody
    public R getServerStateInfo(){
        return R.ok().put("data","success");
    }
}
