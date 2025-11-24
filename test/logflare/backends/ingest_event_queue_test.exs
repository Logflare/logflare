defmodule Logflare.Backends.IngestEventQueueTest do
  use Logflare.DataCase

  alias Logflare.PubSubRates
  alias Logflare.Backends.IngestEventQueue.BufferCacheWorker
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
  alias Logflare.Backends.IngestEventQueue.MapperJanitor
  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  setup do
    insert(:plan)
    :ok
  end

  test "get_table_size/1 returns nil for non-existing tables" do
    assert nil == IngestEventQueue.get_table_size({1, 2, 4})
  end

  test "upsert_tid/1 will recreate a new ets table if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    assert {:ok, new_tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    assert new_tid != tid
  end

  test "get_tid/1 will return nil if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    assert ^tid = IngestEventQueue.get_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    assert nil == IngestEventQueue.get_tid({source.id, backend.id, pid})
  end

  describe "with user, source, backend" do
    setup do
      user = insert(:user)
      [source: insert(:source, user: user), backend: insert(:backend, user: user)]
    end

    test "list_queues/1 returns list of table keys", %{source: source, backend: backend} do
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.34>")})
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.35>")})
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.36>")})
      assert IngestEventQueue.list_queues({source.id, backend.id}) |> length() == 3
    end

    test "list_pending_counts/1 returns list of counts", %{source: source, backend: backend} do
      key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])
      assert [{_, 1}] = IngestEventQueue.list_pending_counts({source.id, backend.id})

      IngestEventQueue.mark_ingested(key, [le])
      assert [{_, 0}] = IngestEventQueue.list_pending_counts({source.id, backend.id})
    end

    test "list_pending_counts/1 does not include uninitialized tables", %{
      source: source,
      backend: backend
    } do
      assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, self()})
      # kill the table
      :ets.delete(tid)
      assert [] == IngestEventQueue.list_pending_counts({source.id, backend.id})
    end

    test "list_counts/1 returns list of counts", %{
      source: %{id: source_id},
      backend: %{id: backend_id}
    } do
      pid = self()
      key = {source_id, backend_id, pid}
      assert {:ok, _tid} = IngestEventQueue.upsert_tid(key)

      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])
      IngestEventQueue.mark_ingested(key, [le])

      assert [
               {{^source_id, ^backend_id, ^pid}, 1}
             ] =
               IngestEventQueue.list_counts({source_id, backend_id})
    end

    test "list_counts/1 does not include uninitialized tables", %{source: source} do
      assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, nil, self()})
      # kill the table
      :ets.delete(tid)
      assert [] == IngestEventQueue.list_counts({source.id, nil})
    end

    test "queues_pending_size/1 returns counts across all queues", %{
      source: %{id: source_id},
      backend: %{id: backend_id}
    } do
      pid = self()
      IngestEventQueue.upsert_tid({source_id, backend_id, pid})
      IngestEventQueue.upsert_tid({source_id, backend_id, nil})
      IngestEventQueue.upsert_tid({source_id, nil, nil})

      IngestEventQueue.add_to_table({source_id, backend_id, nil}, [build(:log_event)])
      IngestEventQueue.add_to_table({source_id, backend_id, self()}, [build(:log_event)])
      assert IngestEventQueue.queues_pending_size({source_id, backend_id}) == 2

      IngestEventQueue.add_to_table({source_id, nil, nil}, [build(:log_event)])
      assert IngestEventQueue.queues_pending_size({source_id, nil}) == 1
    end
  end

  describe "startup queue" do
    setup do
      user = insert(:user)
      sbp = {insert(:source, user: user).id, insert(:backend, user: user).id, nil}
      IngestEventQueue.upsert_tid(sbp)
      [queue: sbp]
    end

    test "adding to startup queue", %{queue: {sid, bid, _} = queue} do
      le = build(:log_event, message: "123")
      assert :ok = IngestEventQueue.add_to_table(queue, [le])
      assert IngestEventQueue.total_pending(queue) == 1
      assert IngestEventQueue.total_pending({sid, bid}) == 1
      assert IngestEventQueue.total_pending({sid, bid, nil}) == 1
    end

    test "move/1 moves all events from one queue to target queue", %{queue: {sid, bid, _} = queue} do
      target = {sid, bid, self()}
      le = build(:log_event, message: "123")
      IngestEventQueue.upsert_tid(target)
      assert :ok = IngestEventQueue.add_to_table(queue, [le])
      assert IngestEventQueue.total_pending(queue) == 1
      assert IngestEventQueue.total_pending(target) == 0
      assert {:ok, 1} = IngestEventQueue.move(queue, target)
      assert IngestEventQueue.total_pending(queue) == 0
      assert IngestEventQueue.total_pending(target) == 1
      assert IngestEventQueue.total_pending({sid, bid}) == 1
      assert IngestEventQueue.total_pending({sid, bid, nil}) == 0
    end
  end

  describe "with a queue" do
    setup do
      user = insert(:user)
      sbp = {insert(:source, user: user).id, insert(:backend, user: user).id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [source_backend_pid: sbp]
    end

    test "object lifecycle", %{source_backend_pid: sbp} do
      le = build(:log_event)
      # insert to table
      assert :ok = IngestEventQueue.add_to_table(sbp, [le])
      assert IngestEventQueue.get_table_size(sbp) == 1
      # can take pending items
      assert {:ok, [_]} = IngestEventQueue.take_pending(sbp, 5)
      assert IngestEventQueue.total_pending(sbp) == 1
      # set to ingested
      assert {:ok, 1} = IngestEventQueue.mark_ingested(sbp, [le])
      assert IngestEventQueue.total_pending(sbp) == 0
      # truncate to n items
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 1)
      assert IngestEventQueue.get_table_size(sbp) == 1
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 0)
      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "drop n items from a queue", %{source_backend_pid: sbp} do
      batch = for _ <- 1..500, do: build(:log_event)
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert {:ok, 2} = IngestEventQueue.drop(sbp, :all, 2)
      assert IngestEventQueue.get_table_size(sbp) == 498
      assert {:ok, 0} = IngestEventQueue.drop(sbp, :ingested, 2)
      assert IngestEventQueue.get_table_size(sbp) == 498
    end

    test "truncate all events in a queue", %{source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :all, 50)
      assert IngestEventQueue.get_table_size(sbp) == 50
      assert :ok = IngestEventQueue.truncate_table(sbp, :all, 0)
      assert IngestEventQueue.get_table_size(sbp) == 0
    end

    test "truncate ingested events in a queue", %{source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert {:ok, _} = IngestEventQueue.mark_ingested(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 50)
      assert IngestEventQueue.get_table_size(sbp) == 50
      assert IngestEventQueue.total_pending(sbp) == 0
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 0)
      assert IngestEventQueue.get_table_size(sbp) == 0
    end

    test "truncate pending events in a queue", %{source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :pending, 50)
      assert IngestEventQueue.total_pending(sbp) == 50
      assert :ok = IngestEventQueue.truncate_table(sbp, :pending, 0)
      assert IngestEventQueue.total_pending(sbp) == 0
    end
  end

  test "BufferCacheWorker caches buffer lengths every n seconds" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}
    other_table = {source.id, nil, pid}

    IngestEventQueue.upsert_tid(table)
    IngestEventQueue.upsert_tid(other_table)
    start_supervised!({BufferCacheWorker, interval: 100})

    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.add_to_table(other_table, [le])
    :timer.sleep(300)

    # Verify worker cached the values automatically (without manual cache calls)
    assert PubSubRates.Cache.get_cluster_buffers(source.id, backend.id) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 1
  end

  test "QueueJanitor cleans up :ingested events" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, remainder: 0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 0
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor leaves remainder of :ingested events if default backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    pid = self()

    table = {source.id, nil, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor,
       source: source, backend: %Backends.Backend{id: nil}, interval: 50, remainder: 10}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 1
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor cleans up all :ingested events if not default backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, remainder: 10}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 0
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor purges if exceeds max" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    batch = for _ <- 1..105, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source.id, backend.id, pid}, batch)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 105

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 100, purge_ratio: 1.0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 0
  end

  test "QueueJanitor purges based on purge ratio" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    batch = for _ <- 1..100, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source.id, backend.id, pid}, batch)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 100

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 90, purge_ratio: 0.5}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 50
  end

  test "MapperJanitor cleans up stale tids" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    tid = IngestEventQueue.get_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    start_supervised!({MapperJanitor, interval: 100})
    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == nil
    assert :ets.info(:ingest_event_queue_mapping, :size) == 0
  end

  describe "IngestEventQueue" do
    @describetag :benchmark
    @describetag timeout: :infinity
    @describetag :skip

    # Benchmark results
    # drop_with_chunking is ~15.65x slower (100 chunk)
    # select and drop is far superior.
    # There is no significant difference in ips between different chunk sizes
    # reductions for drop_with_chunking is 1.5x higher
    # memory usage for both approaches are identical
    #
    # comparison against drop using a select-key matchspec vs select-object
    # selecting the key results in a very tiny ips improvement, not significant at high table sizes.
    test "drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id}

      Benchee.run(
        %{
          # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 100)
          # end,
          # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 500)
          # end,
          # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 1000)
          # end,
          # "select and drop" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sbp, :all, to_drop, nil)
          # end,
          # "select and drop with select-key" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sbp, :all, to_drop, :select_key)
          # end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, 500}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results:
    # using update_element to update the element inplace is hands down superior
    # uses >30-60x less memory, effect increases as with higher batch sizes
    # >8-10.5x more ips, consistent across all batch sizes
    # reductions are the same across all 3.
    test "mark_ingested" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id}

      Benchee.run(
        %{
          # "mark with :ets.insert/2 batched" => fn {input, _} ->
          #   IngestEventQueue.mark_ingested(sbp, input)
          # end,
          # "mark with :ets.insert/2 individually" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_insert_individually(sbp, input)
          # end,
          # "mark with :ets.update_element/2" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_update_element(sbp, input)
          # end,
        },
        inputs: %{
          "1k" => for(_ <- 1..1_000, do: build(:log_event)),
          "500" => for(_ <- 1..500, do: build(:log_event)),
          "100" => for(_ <- 1..100, do: build(:log_event)),
          "10" => for(_ <- 1..10, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results
    # truncation traversal is around ~8x faster
    # memory usage for traversal is ~38x less for large queues
    # reductions  for traversals is 2x less
    test "truncate" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      sbp = {source.id, backend.id, pid}
      {:ok, tid} = IngestEventQueue.upsert_tid(sbp)

      Benchee.run(
        %{
          # "truncate/3 with match_object/3, insert/2, and match_delete/3" => fn {input, _} ->
          #   IngestEventQueue.truncate_no_traversal(sbp, :all, 100)
          # end,
          "mark with :ets.select/3 and traversal" => fn {_input, _to_drop} ->
            IngestEventQueue.truncate_table(sbp, :all, 100)
          end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  # Post-benchmark results:
  # truncate is way slower at large queue sizes, ~x5 slower for 50k. 10k and 1k are comparably close for both.
  # slightly less reductions for drop
  # memory consumption is identical.
  describe "QueueJanitor" do
    @describetag :benchmark
    @describetag timeout: :infinity
    @describetag :skip

    test "truncate vs drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id, pid}

      _state = %{
        source_id: source.id,
        backend_id: backend.id,
        remainder: 100,
        max: 500,
        purge_ratio: 0.1
      }

      Benchee.run(
        %{
          # "work - truncate" => fn {_input, _resource} ->
          #   QueueJanitor.do_work(state)
          # end,
          # "work - drop" => fn {_input, _resource} ->
          #   QueueJanitor.do_drop(state)
          # end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        # memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  # @tag :benchmark
  # @tag timeout: :infinity
  # # @tag :skip
  # # Benchmark results
  # describe "DemandWorker" do
  #   test "fetch" do
  #     user = insert(:user)
  #     source = insert(:source, user: user)
  #     backend = insert(:backend, user: user)
  #     {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
  #     sbp = {source.id, backend.id}

  #     Benchee.run(
  #       %{

  #         # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 100)
  #         # end,
  #         # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 500)
  #         # end,
  #         # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 1000)
  #         # end,
  #         # "select and drop" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sbp, :all, to_drop, nil)
  #         # end,
  #         # "select and drop with select-key" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sbp, :all, to_drop, :select_key)
  #         # end
  #       },
  #       inputs: %{
  #         "50k" => for(_ <- 1..50_000, do: build(:log_event)),
  #         "10k" => for(_ <- 1..10_000, do: build(:log_event)),
  #         "1k" => for(_ <- 1..1_000, do: build(:log_event))
  #       },
  #       # insert the batch
  #       before_scenario: fn input ->
  #         :ets.delete_all_objects(tid)
  #         IngestEventQueue.add_to_table(sbp, input)
  #         {input, 500}
  #       end,
  #       time: 3,
  #       warmup: 1,
  #       memory_time: 3,
  #       reduction_time: 3,
  #       print: [configuration: false],
  #       # use extended_statistics to view units of work done
  #       formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
  #     )
  #   end
  # end
end
