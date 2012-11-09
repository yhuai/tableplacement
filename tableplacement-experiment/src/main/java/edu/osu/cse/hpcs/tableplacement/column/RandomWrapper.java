package edu.osu.cse.hpcs.tableplacement.column;

import java.util.Random;

public abstract class RandomWrapper<T> {

  protected int range;
  protected Random random;

  public abstract T nextValue();
}
