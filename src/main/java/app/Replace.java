package app;

import java.io.File;

public class Replace {
    public static void main(String[] args) {
       String c="/" ;
        String b="/uploads/pd/七都镇人民政府/1973/ 七都镇人民政府 关于要求对雷挺明、雷美凤夫妇非法购买 米非司酮和米索前列醇终止妊娠行为进行立案查处的报告/711-QDZ·2016-B-宁德档案管理局--.pdf";
        String a="\\uploads\\pdf\\七都镇人民政府\\2016\\ 签发人_刘晓东 七都镇人民政府关于要求 协调解决三峡移民子女姜海婷人学问题的请示\\711-QDZ·2016-B-宁德档案管理局--.pdf";
        System.out.println(new File(a.replace("\\", "/").replace(":","_")));
        System.out.println(new File(b.replace("\\", "/").replace(":","_")));
    }
}
