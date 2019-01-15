package com.spark.common;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 定义日志收集客户端收集得到的用户数据参数的name名称<br/>
 * 以及event_logs这张hbase表的结构信息<br/>
 * 用户数据参数的name名称就是event_logs的列名
 * @author zhangcheng
 */
public class EventLogConstants {
    /**
     * 事件枚举类。指定事件的名称
     * 
     * @author gerry
     *
     */
    public enum EventEnum {
        // launch事件，表示第一次安装app访问
        LAUNCH(1, "launch event", "e_la"),
        // 启动app事件
        START(2, "start app event", "e_st"),
        // 页面浏览事件
        PAGEVIEW(3, "page view event", "e_pvi"),
        // 注册账户事件
        REGISTER(4, "register on app event", "e_re")
        ;

        /**
         *  id 唯一标识
         */
        public final int id;
        /**
         * 名称
         */
        public final String name;
        /**
         * 别名，用于数据收集的简写
         */
        public final String alias;

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 获取匹配别名的event枚举对象，如果最终还是没有匹配的值，那么直接返回null。
         * 
         * @param alias 事件简称
         * @return
         */
        public static EventEnum valueOfAlias(String alias) {
            for (EventEnum event : values()) {
                if (event.alias.equals(alias)) {
                    return event;
                }
            }
            return null;
        }
    }

    /**
     * 平台名称常量类
     */
    public static class PlatformNameConstants {
        public static final String PC_WEBSITE_SDK = "website";
        public static final String JAVA_SERVER_SDK = "java_server";
    }

    /**
     * 表名称 event_logs
     */
    public static final String HBASE_NAME_EVENT_LOGS = "event_logs";

    /**
     * event_logs表的列簇名称 info
     */
    public static final String EVENT_LOGS_FAMILY_NAME = "info";

    /**
     * event_logs表列簇对应的字节数组
     */
    public static final byte[] EVENT_LOGS_FAMILY_NAME_BYTES = Bytes.toBytes(EVENT_LOGS_FAMILY_NAME);

    /**
     * 日志分隔符
     */
    public static final String LOG_SEPA_A = "\\^A";
    public static final String LOG_SEPA_AND = "&";
    public static final String LOG_PARAM_EQUAL = "=";
    public static final String LOG_PARAM_CHARSET = "UTF-8";

    /**
     * 用户ip地址
     */
    public static final String LOG_COLUMN_NAME_IP = "ip";

    /**
     * 服务器时间
     */
    public static final String LOG_COLUMN_NAME_SERVER_TIME = "s_time";

    /**
     * 事件名称
     */
    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";

    /**
     * 数据收集端的版本信息
     */
    public static final String LOG_COLUMN_NAME_VERSION = "ver";

    /**
     * 用户唯一标识符
     */
    public static final String LOG_COLUMN_NAME_UUID = "uuid";

    /**
     * 账户唯一标识符
     */
    public static final String LOG_COLUMN_NAME_USER_ID = "user_id";

    /**
     * 会话id
     */
    public static final String LOG_COLUMN_NAME_SESSION_ID = "u_sd";

    /**
     * 客户端时间
     */
    public static final String LOG_COLUMN_NAME_CLIENT_TIME = "c_time";

    /**
     * 语言
     */
    public static final String LOG_COLUMN_NAME_LANGUAGE = "l";

    /**
     * 浏览器user agent参数
     */
    public static final String LOG_COLUMN_NAME_USER_AGENT = "b_iev";

    /**
     * 浏览器分辨率大小
     */
    public static final String LOG_COLUMN_NAME_RESOLUTION = "b_rst";

    /**
     * 定义platform
     */
    public static final String LOG_COLUMN_NAME_PLATFORM = "pl_id";

    /**
     * 当前url
     */
    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";

    /**
     * 前一个页面的url
     */
    public static final String LOG_COLUMN_NAME_REFERRER_URL = "p_ref";

    /**
     * 当前页面的title
     */
    public static final String LOG_COLUMN_NAME_TITLE = "tt";

    /**
     * action名称
     */
    public static final String LOG_COLUMN_NAME_EVENT_ACTION = "ac";

    /**
     * kv前缀
     */
    public static final String LOG_COLUMN_NAME_EVENT_KV_START = "kv_";

    /**
     * duration持续时间
     */
    public static final String LOG_COLUMN_NAME_EVENT_DURATION = "du";

    /**
     * 操作系统名称
     */
    public static final String LOG_COLUMN_NAME_OS_NAME = "os";

    /**
     * 操作系统版本
     */
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_v";

    /**
     * 浏览器名称
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser";

    /**
     * 浏览器版本
     */
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_v";

    /**
     * ip地址解析的所属国家
     */
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";

    /**
     * ip地址解析的所属省份
     */
    public static final String LOG_COLUMN_NAME_PROVINCE = "province";

    /**
     * ip地址解析的所属城市
     */
    public static final String LOG_COLUMN_NAME_CITY = "city";

    /**
     * 应用
     */
    public static final String LOG_COLUMN_NAME_APP = "app_id";

    /**
     * 客户端渠道
     */
    public static final String LOG_COLUMN_NAME_CHANNEL = "ch";

    /**
     * 运营商
     */
    public static final String LOG_COLUMN_NAME_ISP = "isp";

    /**
     * 搜索词类型
     */
    public static final String LOG_COLUMN_NAME_AC = "ac";

    /**
     * 热词
     */
    public static final String LOG_COLUMN_NAME_KW = "kw";

    /**
     * 版本升级前的旧版本号
     */
    public static final String LOG_COLUMN_NAME_P_VER = "p_ver";
}
