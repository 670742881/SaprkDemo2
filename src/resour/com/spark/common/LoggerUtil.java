package com.spark.common;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * 日志抽取工具类
 * 
 */
public class LoggerUtil {
    private static final Logger logger = Logger.getLogger(LoggerUtil.class);

    /**
     *
     * 解析日志，返回一个map集合，如果处理失败，那么empty的map集合<br/>
     * 失败：分割失败，url解析出现问题等等
     * 
     * @param logText 日志
     * @return  行日志，转成map对象，如果业务日志没有（？后面没有），返回map为null
     */
    public static Map<String, String> handleLog(String logText) {
        Map<String, String> logMap = new HashMap<String, String>();
        if (StringUtils.isNotBlank(logText)) {
            String[] splits = logText.trim().split(EventLogConstants.LOG_SEPA_A);
            if (splits.length == 3) {// 数据格式正确，可以分割
                // 第一、二列：IP地址
                logMap.put(EventLogConstants.LOG_COLUMN_NAME_IP, splits[0].trim());
                // 第三列：服务器时间
                long nginxTime = TimeUtil.parseNginxServerTime2Long(splits[1].trim());
                if (nginxTime != -1) {
                    logMap.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(nginxTime));
                }
                // 第四列：处理业务请求参数
                String requestStr = splits[2];
                int index = requestStr.indexOf("?");
                if (index > -1) {
                    // 有请求参数的情况下，获取？后面的参数
                    String requestBody = requestStr.substring(index + 1);
                    // 处理请求参数
                    handleRequestBody(logMap, requestBody);
                } else {
                    // 没有请求参数
                    logMap.clear();
                }
            }
        }
        return logMap;
    }

    /**
     * 处理业务请求参数
     * @param logMap
     * @param requestBody
     */
    private static void handleRequestBody(Map<String, String> logMap, String requestBody) {
        String[] requestParames = requestBody.split(EventLogConstants.LOG_SEPA_AND);
        for (String param : requestParames) {
            if (StringUtils.isNotBlank(param)) {
                int index = param.indexOf(EventLogConstants.LOG_PARAM_EQUAL);
                if (index < 0) {
                    logger.debug("没法进行解析:" + param);
                    continue;
                }
                String key, value = null;
                try {
                    key = param.substring(0, index);
                    value = URLDecoder.decode(param.substring(index + 1), EventLogConstants.LOG_PARAM_CHARSET);
                } catch (Exception e) {
                    logger.debug("value值decode时候出现异常", e);
                    continue;
                }
                // key 和 value都不为空时，保留到map
                if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                    logMap.put(key, value);
                }
            }
        }
    }
}
