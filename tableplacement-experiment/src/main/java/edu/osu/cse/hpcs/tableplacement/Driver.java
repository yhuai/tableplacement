package edu.osu.cse.hpcs.tableplacement;

import org.apache.hadoop.util.ProgramDriver;

import edu.osu.cse.hpcs.tableplacement.rcfile.ReadRCFile;
import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFile;
import edu.osu.cse.hpcs.tableplacement.trevni.ReadTrevniColumnOriented;
import edu.osu.cse.hpcs.tableplacement.trevni.ReadTrevniRowOriented;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevni;

public class Driver {
  public static void main(String[] argv) throws Exception {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("WriteRCFile", WriteRCFile.class,
          "WriteRCFile");
      pgd.addClass("ReadRCFile", ReadRCFile.class,
          "ReadRCFile");

      pgd.addClass("WriteTrevni", WriteTrevni.class,
          "WriteTrevni");
      pgd.addClass("ReadTrevniRowOriented", ReadTrevniRowOriented.class,
          "ReadTrevniRowOriented");
      pgd.addClass("ReadTrevniColumnOriented", ReadTrevniColumnOriented.class,
          "ReadTrevniColumnOriented");

      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);

  }
}
