package edu.osu.cse.hpcs.tableplacement;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
  public static void main(String[] argv) throws Exception {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      //pgd.addClass("yourClass", yourClass.class,
      //    "description");
      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);

  }
}
