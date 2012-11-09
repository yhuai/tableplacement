package edu.osu.cse.hpcs.tableplacement.column;

import org.apache.commons.lang3.RandomStringUtils;

public class StringRandom extends RandomWrapper {

  public StringRandom(int length) {
    this.range = length;
  }

  @Override
  public Object nextValue() {
    return RandomStringUtils.randomAlphanumeric(range);
  }
  
  @Override
  public String toString() {
    return "StringRandom[length=" + range + "]";
  }
}
