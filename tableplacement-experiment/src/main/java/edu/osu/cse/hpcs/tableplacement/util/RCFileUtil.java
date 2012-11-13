package edu.osu.cse.hpcs.tableplacement.util;

import java.util.ArrayList;

public class RCFileUtil {
  public static ArrayList<Integer> parseRCFileReadColumnStr(String readColumnStr) {
    ArrayList<Integer> columns = new ArrayList<Integer>();
    String[] strArray = readColumnStr.split(",");
    for (int i=0; i<strArray.length; i++) {
      columns.add(Integer.valueOf(strArray[i]));
    }
    return columns;
  }
}
