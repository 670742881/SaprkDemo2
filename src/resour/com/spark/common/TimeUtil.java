package com.spark.common;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间控制工具类
 * 
 */
public class TimeUtil {
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String HBASE_TABLE_NAME_SUFFIX_FORMAT = "yyyyMMdd";
    public static final String DATE_TIME_MS_PATTERN = "yyyy-MM-dd HH:mm:ss.S";

    /**
     * 获取昨日的日期格式字符串数据
     * 
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }

    /**
     * 获取对应格式的时间字符串
     * 
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     * 
     * @param input
     * @return
     */
    public static boolean isValidateRunningDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }

    /**
     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
     * 
     * @param input
     * @return
     */
    public static long parseString2Long(String input) {
        return parseString2Long(input, DATE_FORMAT);
    }

    /**
     * 将指定格式的时间字符串转换为时间戳
     * 
     * @param input
     * @param pattern
     * @return
     */
    public static long parseString2Long(String input, String pattern) {
        Date date = null;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }

    /**
     * 将时间戳转换为yyyy-MM-dd格式的时间字符串
     * 
     * @param input
     * @return
     */
    public static String parseLong2String(long input) {
        return parseLong2String(input, DATE_FORMAT);
    }
    public static String parseLong2StringFull(long input) {
        return parseLong2String(input, DATE_TIME_MS_PATTERN);
    }

    /**
     * 将时间戳转换为指定格式的字符串
     * 
     * @param input
     * @param pattern
     * @return
     */
    public static String parseLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 将nginx服务器时间转换为时间戳，如果说解析失败，返回-1
     * 
     * @param input
     * @return
     */
    public static long parseNginxServerTime2Long(String input) {
        Date date = parseNginxServerTime2Date(input);
        return date == null ? -1L : date.getTime();
    }

    /**
     * 将nginx服务器时间转换为date对象。如果解析失败，返回null
     * 
     * @param input
     *            格式: 1449410796.976
     * @return
     */
    public static Date parseNginxServerTime2Date(String input) {
        if (StringUtils.isNotBlank(input)) {
            try {
                long timestamp = Double.valueOf(Double.valueOf(input.trim()) * 1000).longValue();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timestamp);
                return calendar.getTime();
            } catch (Exception e) {
                // nothing
            }
        }
        return null;
    }

    /**
     * 从时间戳中获取需要的时间信息
     * 
     * @param time
     *            时间戳
     * @param type
     * @return 如果没有匹配的type，抛出异常信息
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (DateEnum.YEAR.equals(type)) {
            // 需要年份信息
            return calendar.get(Calendar.YEAR);
        } else if (DateEnum.SEASON.equals(type)) {
            // 需要季度信息
            int month = calendar.get(Calendar.MONTH) + 1;
            if (month % 3 == 0) {
                return month / 3;
            }
            return month / 3 + 1;
        } else if (DateEnum.MONTH.equals(type)) {
            // 需要月份信息
            return calendar.get(Calendar.MONTH) + 1;
        } else if (DateEnum.WEEK.equals(type)) {
            // 需要周信息
            return calendar.get(Calendar.WEEK_OF_YEAR);
        } else if (DateEnum.DAY.equals(type)) {
            return calendar.get(Calendar.DAY_OF_MONTH);
        } else if (DateEnum.HOUR.equals(type)) {
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw new RuntimeException("没有对应的时间类型:" + type);
    }

    /**
     * 获取time指定周的第一天的时间戳值
     * 
     * @param time
     * @return
     */
    public static long  getFirstDayOfThisWeek(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * @param date
     *            : long date like "1360252800000L"
     * @return : long of first day of this month
     */
    public static long getFirstDayOfThisMonth(long date) {
        Date d = new Date();
        d.setTime(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取给定时间上一个月的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfPreviousMonth(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.MONDAY, -1); // 月份减少1
        cal.set(Calendar.DAY_OF_MONTH, 1); // 天设置为月的第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取下个月的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfNextMonth(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.MONDAY, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取上一周的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfPreviousWeek(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, -1); // 周数减少1
        cal.set(Calendar.DAY_OF_WEEK, 1); // 设置获取第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取相对当前周的任意周的第一天，num为正数，则为下一周，为负数，则为上一周
     */
    public static long getFirstDayOfOtherWeek(long date, int num) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, num); // 周数减少1
        cal.set(Calendar.DAY_OF_WEEK, 1); // 设置获取第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取相对当前周的任意周的最后一天，num为正数，则为下一周，为负数，则为上一周
     */
    public static long getLastDayOfOtherWeek(long date, int num) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, num); // 周数减少1
        cal.set(Calendar.DAY_OF_WEEK, 7); // 设置获取第一天
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取下一周的第一天
     * 
     * @param date
     * @return
     */
    public static long getFirstDayOfNextWeek(long date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date);
        cal.add(Calendar.WEEK_OF_YEAR, 1);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 获取当天的毫米及
     * 
     * @return
     */
    public static long getTodayInMillis() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * @param amount
     *            : amount day before or after.
     * @param today
     *            : long date like 1360252800000L
     * @return : string of long date like 1360252800000 <br />
     *         example : getSpecifiedDate (today, 2) : get the date of the day
     *         after tomorrow<br />
     *         getSpecifiedDate (today, -1) : get the date of yesterday
     */
    public static long getSpecifiedDate(long today, int amount) {
        Date d = new Date();
        d.setTime(today);
        Calendar calStart = Calendar.getInstance();
        calStart.setTime(d);
        calStart.add(Calendar.DAY_OF_YEAR, amount);
        calStart.set(Calendar.HOUR_OF_DAY, 0);
        calStart.set(Calendar.MINUTE, 0);
        calStart.set(Calendar.SECOND, 0);
        calStart.set(Calendar.MILLISECOND, 0);
        return calStart.getTimeInMillis();
    }
    public static long getFirstDayOfPreWeek(long date,int preWeekNum) {
        Calendar cal = Calendar.getInstance();
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        int flag = 1;
        flag = parsePreWeekNum(flag,preWeekNum);
        int offset1 = flag - dayOfWeek;
        cal.add(Calendar.DATE, offset1 - 7);
        return cal.getTimeInMillis();
    }
    public static long getEndDayOfPreWeek(long date,int preWeekNum) {
        Calendar cal = Calendar.getInstance();
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        int flag = 7;
        flag = parsePreWeekNum(flag,preWeekNum);
        int offset2 = flag - dayOfWeek;
        cal.add(Calendar.DATE, offset2 - 7);
        return cal.getTimeInMillis();
    }
    private static int parsePreWeekNum(int flag ,int preWeekNum){
        switch (preWeekNum){
            default:// 上1周
                break;
            case 2: // 上2周
                flag = flag - 7;
                break;
            case 3: // 上3周
                flag = flag - 14;
                break;
            case 4: // 上4周
                flag = flag - 21;
                break;
        }
        return flag;
    }

    public static List<String> getDateList(long date1Long, long date2Long, String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        List<String> dateList = new ArrayList<String>();

        Date date1 = new Date(date1Long <= date2Long ? date1Long : date2Long);
        Date date2 = new Date(date2Long > date1Long ? date2Long : date1Long);

        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);

        int day1= cal1.get(Calendar.DAY_OF_YEAR);
        int day2 = cal2.get(Calendar.DAY_OF_YEAR);

        int year1 = cal1.get(Calendar.YEAR);
        int year2 = cal2.get(Calendar.YEAR);

        int dayDistance = 0;   //相差天数
        if(year1 != year2) {    //不同年
            for(int i = year1 ; i < year2 ; i ++) {
                if(i%4==0 && i%100!=0 || i%400==0) {    //闰年
                    dayDistance += 366;
                } else {
                    dayDistance += 365;
                }
            }
        } else {    //相同年
            dayDistance = day2 - day1;
        }
        dateList.add(sdf.format(cal1.getTime()));
        for (int i = 0; i < dayDistance; i++) {
            cal1.add(Calendar.DATE, 1);
            dateList.add(sdf.format(cal1.getTime()));
        }
        return dateList;
    }

    public static void main(String[] a){
        System.out.println(30 * GlobalConstants.DAY_OF_MILLISECONDS);
        System.out.println(parseNginxServerTime2Date("1516291207.533"));
    }
}
