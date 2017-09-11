package main.java;

import org.apache.xerces.dom.PSVIAttrNSImpl;

/**
 * Created by Administrator on 2016/12/16.
 */
public class HdfsTest {


    public static void main(String[] args) {
        String s = "[00-08-22-0A-F0-FB,22,0,16,820]";


        System.out.println(s.split(",").length+"-----"+s.substring(1,s.length()-1));

    }

}
