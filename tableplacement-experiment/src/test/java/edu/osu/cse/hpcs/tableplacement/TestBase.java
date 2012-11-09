package edu.osu.cse.hpcs.tableplacement;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.log4j.PropertyConfigurator;

public abstract class TestBase {
  protected String resourceDir;

  public TestBase() throws URISyntaxException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource("log4j.properties");
    File file = new File(url.toURI());
    resourceDir = file.getParent();
    PropertyConfigurator.configure(url);
  }
}
