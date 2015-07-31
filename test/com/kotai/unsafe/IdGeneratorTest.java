package com.kotai.unsafe;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class IdGeneratorTest {

  public IdGenerator idGenerator = new IdGeneratorWithCustomCASCounter();

  @Test
  public void generate_a_bunch_of_ids_that_are_unique_to_a_client() {
    Set<Long> ids = new HashSet<>();
    for (int i = 0; i <= 20000; i++) {
      long newId = idGenerator.generateUUIDforClient("client");
      assertThat(ids.contains(newId), equalTo(false));
      ids.add(newId);
    }
  }

  @Test
  public void have_2_clients_generate_a_bunch_of_ids_that_are_unique_to_themselves() {
    Set<Long> firstClientIds = new HashSet<>();
    Set<Long> secondClientIds = new HashSet<>();

    for (int i = 0; i <= 20000; i++) {
      long newId = idGenerator.generateUUIDforClient("client1");
      assertThat(firstClientIds.contains(newId), equalTo(false));
      firstClientIds.add(newId);

      long secondId = idGenerator.generateUUIDforClient("client2");
      assertThat(secondClientIds.contains(secondId), equalTo(false));
      secondClientIds.add(secondId);
    }
  }

  @Test
  public void have_a_number_of_threads_generate_a_bunch_of_ids_for_the_same_client() {
    ExecutorService service = Executors.newFixedThreadPool(10);
    Set<Long> synchSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
    List<Future<?>> results = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      results.add(service.submit(new IdGeneratorTask("client1", idGenerator, synchSet)));
    }

    for (int i = 0; i < results.size(); i++) {
      try {
        results.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        fail();
      }
    }

    assertThat(synchSet.size(), equalTo(200000));
  }

  private class IdGeneratorTask implements Runnable {
    private final Set<Long> accumulator;
    private final IdGenerator idGenerator;
    private final String client;

    public IdGeneratorTask(String client, IdGenerator idGenerator, Set<Long> accumulator) {
      this.accumulator = accumulator;
      this.idGenerator = idGenerator;
      this.client = client;
    }

    @Override
    public void run() {
      for (int i = 1; i <= 10000; i++) {
        long newId = idGenerator.generateUUIDforClient(client);
        accumulator.add(newId);
      }
    }
  }
}
