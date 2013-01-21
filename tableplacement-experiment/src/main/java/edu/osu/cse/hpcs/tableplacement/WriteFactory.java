package edu.osu.cse.hpcs.tableplacement;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFile;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevni;
import edu.osu.cse.hpcs.tableplacement.util.CmdTool;

public class WriteFactory {
  public static WriteTo get(String[] args, Class theClass)
      throws IOException, TablePropertyException, SerDeException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {
    Properties cmdProperties = CmdTool.inputParameterParser(args);
    String propertyFilePath = cmdProperties
        .getProperty(CmdTool.TABLE_PROPERTY_FILE);
    String outputPathStr = cmdProperties.getProperty(CmdTool.OUTPUT_FILE);
    boolean getNumberFormatException = false;
    long rowCount = 0;
    try {
      rowCount = Long.valueOf(cmdProperties.getProperty(CmdTool.ROW_COUNT));
    } catch (NumberFormatException e) {
      System.out.println("Row count should be a number");
      getNumberFormatException = true;
    }

    if (getNumberFormatException || propertyFilePath == null
        || outputPathStr == null) {
      System.out.println("usage: " + theClass.getName()
          + " -t <table property file> -o <output> -c <rowCount> [-p ...]");
      System.out.println("You can overwrite properties defined in the "
          + "table property file through '-p key value'");
      System.exit(-1);
    }

    System.out.println("Table property file: " + propertyFilePath);
    System.out.println("Output file: " + outputPathStr);
    System.out.println("Total number of rows: " + rowCount);

    // If we have several formats to support, we use reflection for this part
    WriteTo ret = null;
    if (WriteRCFile.class.equals(theClass)) {
      ret = new WriteRCFile(propertyFilePath, outputPathStr, rowCount,
          cmdProperties);
    } else if (WriteTrevni.class.equals(theClass)) {
      ret = new WriteTrevni(propertyFilePath, outputPathStr, rowCount,
          cmdProperties);
    } else {
      throw new IllegalArgumentException("Class " + theClass.getCanonicalName()
          + " is not supported");
    }
    return ret;
  }
}
