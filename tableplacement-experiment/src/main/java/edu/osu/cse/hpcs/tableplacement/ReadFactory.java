package edu.osu.cse.hpcs.tableplacement;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.rcfile.ReadRCFile;
import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFile;
import edu.osu.cse.hpcs.tableplacement.trevni.ReadTrevni;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevni;
import edu.osu.cse.hpcs.tableplacement.util.CmdTool;

public class ReadFactory {
  public static ReadFrom get(String[] args, Class theClass)
      throws IOException, TablePropertyException, SerDeException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {
    Properties cmdProperties = CmdTool.inputParameterParser(args);
    String propertyFilePath = cmdProperties
        .getProperty(CmdTool.TABLE_PROPERTY_FILE);
    String inputPathStr = cmdProperties.getProperty(CmdTool.INPUT_FILE);

    if (propertyFilePath == null || inputPathStr == null) {
      System.out.println("usage: " + theClass.getName()
          + " -t <table property file> -i <input> [-p ...]");
      System.out.println("You can overwrite properties defined in the "
          + "table property file through '-p key value'");
      System.exit(-1);
    }

    System.out.println("Table property file: " + propertyFilePath);
    System.out.println("Input file: " + inputPathStr);

    // If we have several formats to support, we use reflection for this part
    ReadFrom ret = null;
    if (ReadRCFile.class.equals(theClass)) {
      ret = new ReadRCFile(propertyFilePath, inputPathStr, cmdProperties);
    } else if (ReadTrevni.class.equals(theClass)) {
      ret = new ReadTrevni(propertyFilePath, inputPathStr, cmdProperties);
    } else {
      throw new IllegalArgumentException("Class " + theClass.getCanonicalName()
          + " is not supported");
    }
    return ret;
  }
}
