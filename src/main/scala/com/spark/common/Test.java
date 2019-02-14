package com.spark.common;

import java.io.IOException;
import java.util.Arrays;


public class Test {
    public static void main(String[] args) {
        City city = null;
        try {
            city = new City("E:\\MyProject\\SaprkDemo\\src\\ipData\\17monipdb.datx");
            String[] a = city.find("117.36.172.147");
            System.out.println(a[0]);
            System.out.println(a[1]);
            System.out.println(a[2]);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (IPv4FormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // 城市库
//      System.out.println(Arrays.toString(city.find("116.247.102.210")));
//      System.out.println(Arrays.toString(city.find("255.255.255.255")));
//      System.out.println(Arrays.toString(city.find("219.136.134.157")));
//      System.out.println(Arrays.toString(city.find("61.244.148.166")));
//      System.out.println(Arrays.toString(city.find("114.61.94.253")));
//      System.out.println(Arrays.toString(city.find("39.182.209.13")));
//     System.out.println(Arrays.toString(city.find("10.20.3.169")));
//        String[] a = getInfo("");

        System.out.println(Arrays.toString(getInfo("10.20.3.169")));
        System.out.println(Arrays.toString(getInfo("10.20.3.171")));
//        System.out.println(Arrays.toString(a));
    }


    public static String[] getInfo(String ip) {
        String[] info =new String[3];
        // 城市库
        City city ;
        try {
           // city = new City("/opt/17monipdb.datx");
          city = new City("E:\\MyProject\\SaprkDemo\\src\\ipData\\17monipdb.datx");
         // city = new City("E:\\myproject\\datx-java\\data\\17monipdb.datx");
            if (city != null) {
                //city = new City("/opt/softs/17monipdb.datx");
                //解析ip得到国家 、省 、市数组
                info = city.find(ip);
                if (info != null) {
                    if (info[2] == null || "".equals(info[2])) {
                        info = new String[]{info[0], info[1], "unknow"};
                    }
                }
                else  {
                    info = new String[]{"unknow", "unknow", "unknow"};
                }
            }
            return info;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IPv4FormatException e) {
            System.out.println(ip+"无法解析");
        }
        return new String[]{"unknow", "unknow", "unknow"};

    }
}
//
//            try {
//                district = new District("E:\\myproject\\datx-java\\data\\17monipdb.datx");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//
//            System.out.println(Arrays.toString(district.find("116.247.102.210")));
//            System.out.println(Arrays.toString(district.find("223.255.127.250")));

// BaseStation baseStation = new BaseStation("/path/to/station_ip.datx"); // 基站库
//  System.out.println(Arrays.toString(baseStation.find("8.8.8.8")));
// System.out.println(Arrays.toString(baseStation.find("223.221.121.0")));