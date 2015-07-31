package com.kotai.unsafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class IdGeneratorImpl implements IdGenerator {

  private ConcurrentHashMap<String, AtomicLong> idGenerators = new ConcurrentHashMap<>();

  @Override
  public long generateUUIDforClient(String clientId) {
    if (!idGenerators.containsKey(clientId))
      idGenerators.putIfAbsent(clientId, new AtomicLong());
    AtomicLong idGenerator = idGenerators.get(clientId);
    return idGenerator.getAndIncrement();
  }

}
