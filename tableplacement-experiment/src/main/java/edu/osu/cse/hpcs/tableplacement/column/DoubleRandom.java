package edu.osu.cse.hpcs.tableplacement.column;

import java.util.Random;

public class DoubleRandom extends RandomWrapper {

  public DoubleRandom(int range) {
    this.range = range;
    this.random = new Random();
  }

  @Override
  public Object nextValue() {
    return new Double(random.nextDouble() * range);
  }

  @Override
  public String toString() {
    return "DoubleRandom[range=" + range + "]";
  }
}
