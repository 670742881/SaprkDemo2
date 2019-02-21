//package app;
//import java.io.FileOutputStream;
//import java.io.OutputStream;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.poi.xssf.usermodel.XSSFCell;
//import org.apache.poi.xssf.usermodel.XSSFCellStyle;
//import org.apache.poi.xssf.usermodel.XSSFRichTextString;
//import org.apache.poi.xssf.usermodel.XSSFRow;
//import org.apache.poi.xssf.usermodel.XSSFSheet;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//public class ExcelTest {
//
//
//
//
///**  
//      *  
//      * @param xls  
//      *            XlsDto实体类的一个对象  
//      * @throws Exception  
//      *             在导入Excel的过程中抛出异常  
//      */ 
//   public static void xlsDto2Excel(List<User> xls) throws Exception {  
// // 获取总列数  
// int CountColumnNum = 5;
//        XSSFWorkbook hwb = new XSSFWorkbook();  
// XSSFCellStyle style = hwb.createCellStyle();
//
//
//        style.setAlignment(XSSFCellStyle.ALIGN_CENTER);
//        User user = null;  
//        XSSFSheet sheet = hwb.createSheet();
//        sheet.setColumnWidth(0,256*2*7);//一个汉字两个字符
//        sheet.setColumnWidth(1,256*2*11);
//        sheet.setColumnWidth(2,256*2*6);
//        sheet.setColumnWidth(3,256*2*5);
//        sheet.setColumnWidth(4,256*2*11);
//        // sheet 对应一个工作页  
////        XSSFSheet sheet = hwb.createSheet("pldrxkxxmb"); 
//        XSSFRow firstrow = sheet.createRow(0);
//        XSSFCell[] firstcell = new XSSFCell[CountColumnNum];  
//        String[] names = new String[CountColumnNum];  
//        names[0] = "手机号";  
//        names[1] = "身份证号";  
//        names[2] = "资信平台";  
//        names[3] = "击中规则";  
//        names[4] = "时间";  
//        for (int j = 0; j < CountColumnNum; j++) {  
//            firstcell[j] = firstrow.createCell(j); 
//            firstcell[j].setCellValue(new XSSFRichTextString(names[j]));;  
//            firstcell[j].setCellStyle(style);
//            
//        }  
//        for (int i = 0; i < xls.size(); i++) {  
//            // 创建一行  
//            XSSFRow row = sheet.createRow(i + 1);  
//            // 得到要插入的每一条记录  
//            user = xls.get(i);  
//        XSSFCell xh0 = row.createCell(0);  
//        xh0.setCellValue(user.getId()); 
//        xh0.setCellStyle(style);
//        XSSFCell xh1 = row.createCell(1);  
//        xh1.setCellValue(user.getNo()); 
//        xh1.setCellStyle(style);
//        XSSFCell xh2 = row.createCell(2);  
//        xh2.setCellValue(user.getPlatform());
//        XSSFCell xh3 = row.createCell(3);  
//        xh3.setCellValue(user.getRule()); 
//        XSSFCell xh4 = row.createCell(4);  
//        xh4.setCellValue(user.getTime()); 
//        xh4.setCellStyle(style); 
//            
//        }  
//        // 创建文件输出流，准备输出电子表格 
//        //当HSSF类型的excel时，新建excel需要放在工程下POI2Excel的文件夹中。而为XSSF时，默认放在工程目录下。
//        //HSSF版本的excel(2003)最大可存入65536条记录，而XSSF版本的excel则可存放1048576条记录
//        OutputStream out = new FileOutputStream("user.xlsx");  
//        hwb.write(out);  
//        out.close();  
//        System.out.println("数据库导出成功");  
//    }  
//                    public static void main(String[] args){
//    List<User> list = new ArrayList<User>();
//    for(int i = 0; i< 1000; i++){
//    list.add(new User("13512341204", "441900196706299382", "同盾", "D101", "2017-01-21 18:46:01"));    
//    list.add(new User("15811111326", "340303197703055389", "同盾", "D101", "2017-01-21 18:47:00"));     
//    list.add(new User("13512340067", "653224197705141266", "同盾", "D101", "2017-01-21 18:48:00"));     
//    list.add(new User("15811111335", "320900196710294535", "同盾", "D101", "2017-01-21 18:49:00"));     
//    list.add(new User("17700000424", "510502199512057758", "同盾", "D101", "2017-01-21 18:50:00"));    
//    list.add(new User("14761508264", "150701199107028299", "同盾", "D101", "2017-01-21 18:51:00"));     
//    list.add(new User("14760656721", "632821198703038744", "聚立信", "D101", "2017-01-21 18:52:00"));     
//    list.add(new User("15845611122", "421182198409244752", "同盾", "D101", "2017-01-21 18:53:00"));     
//    list.add(new User("13334557298", "420704197711171695", "同盾", "D101", "2017-01-21 18:55:00")); 
//    }
//    try {
//                xlsDto2Excel(list);
//            } catch (Exception e) {
//// TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//   
//    }
//    }
//    class User{
//        public User(String id,String no, String platform,String rule, String time){
//            this.id = id;
//            this.no = no;
//            this.platform = platform;
//            this.rule = rule;
//            this.time = time;
//        }
//        private String id;
//        private String no;
//        private String platform;
//        private String rule;
//        private String time;
//        public String getId() {
//            return id;
//        }
//
//
//        public void setId(String id) {
//            this.id = id;
//        }
//
//
//        public String getNo() {
//            return no;
//        }
//
//
//        public void setNo(String no) {
//            this.no = no;
//        }
//
//
//        public String getPlatform() {
//            return platform;
//        }
//
//
//        public void setPlatform(String platform) {
//            this.platform = platform;
//        }
//
//
//        public String getRule() {
//            return rule;
//        }
//
//
//        public void setRule(String rule) {
//            this.rule = rule;
//        }
//
//
//        public String getTime() {
//            return time;
//        }
//
//
//        public void setTime(String time) {
//            this.time = time;
//        }
//
//        public String toString(){
//            return "id:" + this.id + ",no" + this.no + ",platform:" + this.platform + ",rule:" + this.rule + ", time:"+ this.time;
//        }
//    }
//
//
//
