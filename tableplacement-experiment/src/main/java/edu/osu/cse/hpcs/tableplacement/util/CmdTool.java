package edu.osu.cse.hpcs.tableplacement.util;

import java.util.Properties;

public class CmdTool {
  
  public final static String TABLE_PROPERTY_FILE = "table.property.file";
  public final static String OUTPUT_FILE = "output.file";
  public final static String ROW_COUNT = "write.row.count";
  
  public static Properties inputParameterParser (String[] args) {
    Properties prop = new Properties();
    int i=0;
    while (i<args.length) {
      if ("-t".equals(args[i])) {
        prop.put(TABLE_PROPERTY_FILE, args[i+1]);
        i += 2;
      } else if ("-o".equals(args[i])) {
        prop.put(OUTPUT_FILE, args[i+1]);
        i += 2;
      } else if ("-c".equals(args[i])) {
        prop.put(ROW_COUNT, args[i+1]);
        i += 2;
      } else if ("-p".equals(args[i])) {
        prop.put(args[i+1], args[i+2]);
        i += 3;
      }
    }
    
    return prop;
  }
}
