package com.kotai.unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import sun.misc.Unsafe;

public class IdGeneratorWithCustomCASCounter implements IdGenerator {

  private ConcurrentHashMap<String, CASCounter> idGenerators = new ConcurrentHashMap<>();

  @Override
  public long generateUUIDforClient(String clientId) {
    if (!idGenerators.containsKey(clientId))
      idGenerators.putIfAbsent(clientId, new CASCounter());
    CASCounter idGenerator = idGenerators.get(clientId);
    return idGenerator.getAndIncrement();
  }

  private class CASCounter {
    private volatile long counter = 0;
    private Unsafe unsafe;
    private long offset;

    public CASCounter() {
      Field f;
      try {
        f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (Unsafe) f.get(null);
        offset = unsafe.objectFieldOffset(CASCounter.class.getDeclaredField("counter"));
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    public long getAndIncrement() {
      long before = counter;
      long next = before + 1;
      while (!unsafe.compareAndSwapLong(this, offset, before, next)) {
        before = counter;
        next = before + 1;
      }
      return next;
    }
  }
}
