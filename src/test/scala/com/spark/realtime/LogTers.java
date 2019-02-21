package com.spark.realtime;

import com.spark.common.LoggerUtil;

public class LogTers {
    public static void main(String[] args) {
        String log="10.20.3.169^A1543507200.659^A/cntv.gif?c_type_name=videoClips&ch_du_time=&status=8&dura=16&c_code=080000034434&c_src=15&operator=&c_name=%E5%8F%AF%E4%BB%A5%E5%BD%93%E6%88%91%E7%9A%84%E6%91%84%E5%BD%B1%E5%B8%88%E5%90%97%EF%BC%9F&cp_id=191629668&price=&bc_refuse_r=&c_type=2&cp_name=%E5%96%94%E5%96%94%E6%AC%A7%E5%B0%BCstina&en=e_dm&c_id=1001818675&pc_type=auto\n";
        System.out.println(LoggerUtil.handleLog(log).toString());
    }
}
