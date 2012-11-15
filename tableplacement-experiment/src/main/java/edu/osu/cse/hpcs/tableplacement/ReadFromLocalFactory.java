package edu.osu.cse.hpcs.tableplacement;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;

import edu.osu.cse.hpcs.tableplacement.exception.TablePropertyException;
import edu.osu.cse.hpcs.tableplacement.rcfile.ReadRCFileFromLocal;
import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFileToLocal;
import edu.osu.cse.hpcs.tableplacement.trevni.ReadTrevniFromLocal;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevniToLocal;
import edu.osu.cse.hpcs.tableplacement.util.CmdTool;

public class ReadFromLocalFactory {
  public static ReadFromLocal get(String[] args, Class theClass)
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
    ReadFromLocal ret = null;
    if (ReadRCFileFromLocal.class.equals(theClass)) {
      ret = new ReadRCFileFromLocal(propertyFilePath, inputPathStr, cmdProperties);
    } else if (ReadTrevniFromLocal.class.equals(theClass)) {
      ret = new ReadTrevniFromLocal(propertyFilePath, inputPathStr, cmdProperties);
    } else {
      throw new IllegalArgumentException("Class " + theClass.getCanonicalName()
          + " is not supported");
    }
    return ret;
  }
}
