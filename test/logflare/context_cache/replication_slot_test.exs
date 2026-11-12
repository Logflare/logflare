defmodule Logflare.ContextCache.ReplicationSlotTest do
  use Logflare.DataCase, async: false

  alias Cainophile.Adapters.Postgres
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Repo.ConnectionOptions
  alias Logflare.TestUtils

  @flush_confirmation_duration 45_000

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

  defp publisher_options do
    config = Application.get_env(:logflare, CacheBuster, [])

    [
      epgsql:
        ConnectionOptions.prepare_replication_epgsql(
          Application.get_env(:logflare, Repo, []),
          config[:db_url_options]
        ),
      slot: config[:replication_slot],
      wal_position: {"0", "0"},
      publications: config[:publications]
    ]
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
