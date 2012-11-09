package edu.osu.cse.hpcs.tableplacement.column;

import java.util.Random;

public class IntRandom extends RandomWrapper<Integer> {

  public IntRandom(int range) {
    this.range = range;
    this.random = new Random();
  }

  @Override
  public Integer nextValue() {
    return new Integer(random.nextInt(range));
  }

  @Override
  public String toString() {
    return "IntRandom[range=" + range + "]";
  }

}
