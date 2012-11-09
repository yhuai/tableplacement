package edu.osu.cse.hpcs.tableplacement;

import org.apache.hadoop.util.ProgramDriver;

import edu.osu.cse.hpcs.tableplacement.rcfile.WriteRCFileToLocal;

public class Driver {
  public static void main(String[] argv) throws Exception {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("WriteRCFileToLocal", WriteRCFileToLocal.class, "WriteRCFileToLocal");
      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);

  }
}
