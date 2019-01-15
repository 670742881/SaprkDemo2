//package app;
//
//import com.spark.common.TimeUtil;
//import org.apache.commons.lang.StringUtils;
//
//import java.net.URLDecoder;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class logTest {
//    public Boolean call(String line) throws Exception {
//        if(line==null || line.trim().length()==0){
//            return false;
//        }
//
//        String [] fields=line.split("\\^A");
//        if(fields.length<3){
//            return false;
//        }
//        String ip=fields[0];
//        //服务器时间
//        Date time= TimeUtil.parseNginxServerTime2Date(fields[1]);
//        if(time==null){
//            return false;
//        }
//        String timeStr="";
//        try {
//            timeStr=sdf.format(time);
//        } catch (Exception e1) {
//            return false;
//        }
//
//        if(timeStr.indexOf(statDay)<0){
//            return false;
//        }
//
//        String url=fields[2];
//        if(!url.contains("/cntv.gif?")){
//            return false;
//        }
//        String params=url.substring("/cntv.gif?".length(),url.length());
//        if(params==null || params.trim().length()==0){
//            return false;
//        }
//        Map<String, String> paramMap=new HashMap<String, String>();
//        ///cntv.gif?后面的日志字段按照"&"来切割,得到所有字段的map集合
//        String[]  paramFileds=params.split("&");
//        for(String param:paramFileds){
//            String [] paramName=param.split("=");
//            try {
//                if(paramName[0]==null || paramName[0].trim().length()==0){
//                    continue;
//                }
//                String paramValue="";
//                if(paramName.length>1){
//                    paramValue=paramName[1];
//                    if(paramValue!=null && paramValue.trim().length()>0){
//                        paramValue= URLDecoder.decode(paramValue.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
//                    }
//                }
//                paramMap.put(paramName[0].toLowerCase(), paramValue);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        //记录服务器时间,也就是访问时间
//        paramMap.put("visittime", timeStr);
//        String regEx = "[~!/@#$%^&*()_=+\\|[{}];:\'\",<.>/?]+";
//        Pattern p = Pattern.compile(regEx);
//        String uuid=paramMap.get("uuid");
//        if(uuid==null || uuid.trim().length()>50){
//            return false;
//        }
//        if (uuid != null) {
//            Matcher m = p.matcher(uuid);
//            if (m.find()) {
//                return false;
//            }
//        }
//        String appId=paramMap.get("appid");
//        if (!"1".equals(appId) && !"2".equals(appId) && !"3".equals(appId)) {
//            return false;
//        }
//        String clientType=paramMap.get("clienttype");
//        if (!"1".equals(clientType) && !"2".equals(clientType) && !"3".equals(clientType)) {
//            return false;
//        }
//        String clientIp=paramMap.get("clientip");
//        if(StringUtils.isBlank(clientIp)){
//            clientIp=ip;
//        }
//		/*if(appId==null || appId.trim().length()==0 ||  clientIp==null || clientIp.trim().length()==0 ||
//				 clientType==null || clientType.trim().length()==0){
//			return false;
//		}
//		if("0".equals(clientType)){
//			return false;
//		}
//		if("0".equals(appId) && Long.valueOf(appId)>3){
//			return false;
//		}*/
//        return true;
//    }
//}
