package edu.osu.cse.hpcs.tableplacement.rcfile;

public class WriteRCFile2 {
  
  public WriteRCFile2 () {
    
  }
  
  
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: " + WriteRCFile2.class.getName() + " <table property file>");
      System.exit(-1);
    }
    
    String propertyFilePath = args[0];
    System.out.println("Using table property file " + propertyFilePath);
    
  }
}
