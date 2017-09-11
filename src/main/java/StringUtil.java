package main.java;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by jxl on 2016/8/23.
 */
public class StringUtil {

    public static void main(String[] args) throws IOException {

        String date = "2016-06-04 00:00:36.0";
        String time = date.toString().split("\\.", 2)[0];
        //System.out.println(new Date(time).getTime());
        //row.put('SUBMIT_TIME', new Date(time).getTime());


        //String inPut = "D://Program Files//Java//JetBrains/workspace//SparkJob//input//table";
        String inPut = "input/Fields";

        String[] arr = {"LAW_PRINCIPAL_CERTIFICATE_ID", "LAW_PRINCIPAL_CERTIFICATE_TYPE", "SERVICE_NAME_PIN_YIN",
                "NET_MONITOR_MAN_TEL", "NET_MONITOR_MAN", "NET_MONITOR_DEPARTMENT", "SERVICE_IP", "INFOR_MAN_EMAIL",
                "INFOR_MAN_TEL", "INFOR_MAN", "PRINCIPAL_TEL", "PRINCIPAL", "ADDRESS", "SERVICE_NAME", "SUMMARY",
                "SENDER_ACCOUNT", "FRIEND_NAME", "SENDER_NAME", "KEYWORD", "FRIEND_ACCOUNT", "ACCOUNT_NAME",
                "CERTIFICATE_CODE", "USER_NAME", "ACCOUNT", "URL", "DOMAIN_NAME", "TITLE", "AUTHOR", "REF_DOMAIN",
                "SERVICE_CODE", "REF_URL", "BUYER_ADDRESS", "DATA_SOURCE", "KEYWORD_CODE", "IDCODE", "SOURCE",
                "SRC_IP", "DEST_IP", "BUYER_EMAIL", "BUYER_PHONE", "BUYER_MOBILE", "BUYER_NAME", "ACOUNT_NAME",
                "SRC_MAC", "ROOM_ID"};

        String[] high = {"SUMMARY", "FILE_NAME", "GAMENAME", "MSG", "KEYWORD", "DOMAIN_NAME"};

        List<String> regex = Arrays.asList(arr);

        Set<String> titles = new HashSet<>();
        Set<String> contents = new HashSet<>();

        try {
            StringBuffer sb = new StringBuffer("");

            FileReader reader = new FileReader(inPut);
            BufferedReader br = new BufferedReader(reader);

            String str = null;
            int i = 0;
            while ((str = br.readLine()) != null) {

                System.out.println("p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(\""+str+"\"), Bytes.toBytes(field("+i+")))");
                i++;
                /*if (regex.contains(str)) {
                    //System.out.print(str+",");
                    System.out.println(str);
                }*/
                //System.out.println("<field column=\"" + str + "\" name=\"" + str + "\" />");

                /* if (regex.contains(str)) {
//                    System.out.println("List<String> "+str+"List = highlighting.get(doc.get(\"ID\")).get(\""+str+"\");");
//                    System.out.println("String "+str+" = \"\";");
//                    //判断是否有高亮信息
//                    System.out.println("if ("+str+"List != null && "+str+"List.size() > 0) {");
//                    System.out.println(str+" = "+str+"List.get(0);");
//                    System.out.println("}");
//                    System.out.println("title.append("+str+"+\" \");");

                    System.out.print(str+ ",");

                    //titles.add(str);
                    continue;
                } else contents.add(str);*/

//                System.out.print("\"" + str + "\",");
                //System.out.print(str + ",");


                //System.out.println("<field name=\""+str+"\" type=\"text_ik\" indexed=\"true\" stored=\"true\"/>");

            }


            for (String title : titles) {
                //System.out.println(setModel("title", title));
            }

            for (String content : contents) {
//                System.out.println(setModel("content", content));
            }


            //sb.deleteCharAt(sb.length()-1);
            //System.out.println(sb.toString());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    static String setModel(String title, String key) {

        //return title + ".set" + key + "((String) doc.getFieldValue(\"" + key + "\"));";

        //title.append(doc.getFieldValue("CERTIFICATE_CODE") + " ");

        return title + ".append(checkEmptyfield(doc.getFieldValue(\"" + key + "\").toString())+ \" \");";

    }
}

