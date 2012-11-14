package edu.osu.cse.hpcs.tableplacement;

import org.apache.hadoop.util.ProgramDriver;

import edu.osu.cse.hpcs.tableplacement.rcfile.ReadRCFileFromLocal;
import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFileToLocal;
import edu.osu.cse.hpcs.tableplacement.trevni.ReadTrevniFromLocal;
import edu.osu.cse.hpcs.tableplacement.trevni.WriteTrevniToLocal;

public class Driver {
  public static void main(String[] argv) throws Exception {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("WriteRCFileToLocal", WriteRCFileToLocal.class,
          "WriteRCFileToLocal");
      pgd.addClass("ReadRCFileFromLocal", ReadRCFileFromLocal.class,
          "ReadRCFileFromLocal");
      
      pgd.addClass("WriteTrevniToLocal", WriteTrevniToLocal.class,
          "WriteTrevniToLocal");
      pgd.addClass("ReadTrevniFromLocal", ReadTrevniFromLocal.class,
          "ReadTrevniFromLocal");
      
      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);

  }
}
