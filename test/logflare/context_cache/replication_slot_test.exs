defmodule Logflare.ContextCache.ReplicationSlotTest do
  use Logflare.DataCase, async: false

  alias Cainophile.Adapters.Postgres
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.GenSingleton
  alias Logflare.TestUtils

  @flush_confirmation_duration 10_000

  setup do
    existing_slots = slot_names()
    publisher = start_supervised!({Postgres, publisher_options()})
    [slot] = slot_names() -- existing_slots

    %{publisher: publisher, slot: slot}
  end

  test "confirms flush past commits that do not touch the publication", %{slot: slot} do
    wal_position = commit_unpublished_write()

    TestUtils.retry_assert([sleep: 500, duration: @flush_confirmation_duration], fn ->
      assert confirmed_past?(slot, wal_position)
    end)
  end

  test "publishes and confirms commits that touch the publication", %{
    publisher: publisher,
    slot: slot
  } do
    Postgres.subscribe(publisher, self())
    wal_position = commit_unpublished_write()

    user = SQL.Sandbox.unboxed_run(Repo, fn -> insert(:user) end)
    on_exit(fn -> SQL.Sandbox.unboxed_run(Repo, fn -> Repo.delete!(user) end) end)
    user_id = to_string(user.id)

    assert_receive %Transaction{
                     changes: [
                       %NewRecord{relation: {"public", "users"}, record: %{"id" => ^user_id}}
                     ]
                   },
                   5_000

    TestUtils.retry_assert(fn -> assert confirmed_past?(slot, wal_position) end)
  end

  test "stops confirming and drops the slot when the publisher stops", %{
    publisher: publisher,
    slot: slot
  } do
    {:ok, test_supervisor} = ExUnit.fetch_test_supervisor()
    %{connection: connection} = :sys.get_state(publisher)
    {:links, links} = Process.info(publisher, :links)
    assert [confirmer] = links -- [test_supervisor, connection]
    confirmer_ref = Process.monitor(confirmer)

    :ok = stop_supervised(Postgres)

    assert_receive {:DOWN, ^confirmer_ref, :process, ^confirmer, _reason}, 5_000
    TestUtils.retry_assert(fn -> refute slot_exists?(slot) end)
  end

  defp publisher_options do
    {:ok, {_flags, children}} = ContextCache.Supervisor.init([])

    children
    |> Enum.find_value(fn
      %{
        start:
          {GenSingleton, :start_link,
           [[child_spec: %{start: {Postgres, :start_link, [options]}}]]}
      } ->
        options

      _child ->
        nil
    end)
    |> Keyword.delete(:register)
  end

  defp slot_names do
    %{rows: rows} =
      Repo.query!("""
      SELECT slot_name::text
      FROM pg_replication_slots
      WHERE database = current_database() AND temporary AND active
      """)

    List.flatten(rows)
  end

  defp slot_exists?(slot) do
    %{rows: [[exists?]]} =
      Repo.query!("SELECT count(*) > 0 FROM pg_replication_slots WHERE slot_name = $1", [slot])

    exists?
  end

  defp commit_unpublished_write do
    SQL.Sandbox.unboxed_run(Repo, fn ->
      Repo.query!("CREATE TEMP TABLE replication_slot_test_wal (id int) ON COMMIT DROP")
      %{rows: [[wal_position]]} = Repo.query!("SELECT pg_current_wal_lsn()::text")
      wal_position
    end)
  end

  defp confirmed_past?(slot, wal_position) do
    %{rows: [[confirmed_past?]]} =
      Repo.query!(
        """
        SELECT confirmed_flush_lsn >= $1::text::pg_lsn
        FROM pg_replication_slots
        WHERE slot_name = $2
        """,
        [wal_position, slot]
      )

    confirmed_past?
  end
end
