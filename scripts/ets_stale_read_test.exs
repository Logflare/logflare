#!/usr/bin/env elixir
# Demonstrates that ets.select with write_concurrency + read_concurrency can return
# stale :pending entries in a subsequent select after update_element already set :processing.
#
# Mirrors IngestEventQueue's tid table setup and take_pending_ids call pattern.
#
# Run with: mix run scripts/ets_stale_read_test.exs

defmodule EtsStaleReadTest do
  @num_events 100_000
  @num_writers 32
  @writer_iterations 50_000
  @select_limit 500
  @num_select_rounds 500

  def run do
    schedulers = System.schedulers_online()
    IO.puts("=== ETS Across-Call Stale Read Test ===")
    IO.puts("Schedulers online: #{schedulers}")
    IO.puts("Table: :set, write_concurrency: :auto, read_concurrency: true\n")

    test_with(:update_element)
    IO.puts("")
    test_with(:select_replace)
  end

  defp test_with(mode) do
    IO.puts("--- Mode: #{mode} ---")

    tid =
      :ets.new(:stale_test, [
        :public,
        :set,
        {:decentralized_counters, false},
        {:write_concurrency, :auto},
        {:read_concurrency, true}
      ])

    for i <- 1..@num_events do
      :ets.insert(tid, {i, :pending, %{}, 100})
    end

    ms_pending = [{{:"$1", :pending, :_, :_}, [], [:"$1"]}]

    # Concurrent writers inserting NEW unique events — causes hash table resizing
    # and bucket splits, which triggers the stale read window.
    writers =
      for w <- 1..@num_writers do
        Task.async(fn ->
          for j <- 1..@writer_iterations do
            # Unique id per writer+iteration, no contention on counter
            new_id = @num_events + w * @writer_iterations + j
            :ets.insert(tid, {new_id, :pending, %{}, 100})
            # Keep table size bounded by deleting old inserts
            :ets.delete(tid, new_id - @num_events)
          end
        end)
      end

    stale_reads =
      for _round <- 1..@num_select_rounds, reduce: 0 do
        acc ->
          # Call 1: select pending ids
          ids =
            case :ets.select(tid, ms_pending, @select_limit) do
              {ids, _cont} -> ids
              :"$end_of_table" -> []
            end

          # Mark all as :processing
          Enum.each(ids, fn id ->
            case mode do
              :update_element ->
                :ets.update_element(tid, id, {2, :processing})

              :select_replace ->
                replace_ms = [
                  {{id, :pending, :"$1", :"$2"}, [],
                   [{:const, {id, :processing, :"$1", :"$2"}}]}
                ]
                :ets.select_replace(tid, replace_ms)
            end
          end)

          # Call 2: select pending again — any ids from Call 1 reappearing = stale read
          ids_set = MapSet.new(ids)

          stale =
            case :ets.select(tid, ms_pending, @select_limit) do
              {ids2, _cont} ->
                Enum.count(ids2, fn id -> MapSet.member?(ids_set, id) end)

              :"$end_of_table" ->
                0
            end

          acc + stale
      end

    Task.await_many(writers, 120_000)

    IO.puts("  Rounds: #{@num_select_rounds}, select limit: #{@select_limit}")
    IO.puts("  Stale reads (id reappeared as :pending after being marked :processing): #{stale_reads}")

    result =
      if stale_reads > 0,
        do: "CONFIRMED - stale read reproduces",
        else: "not reproduced under these conditions"

    IO.puts("  Result: #{result}")
    :ets.delete(tid)
  end
end

EtsStaleReadTest.run()
