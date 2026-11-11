defmodule Logflare.QueryContextTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.ContextCache.Gossip
  alias Logflare.Google.BigQuery
  alias Logflare.QueryContext
  alias Logflare.QueryContext.Cache
  alias Logflare.Sql
  alias Logflare.Users

  setup do
    insert(:plan)
    stub(BigQuery, :create_dataset, fn _, _, _, _ -> {:ok, %{}} end)
    user = insert(:user, bigquery_project_id: "original-project", bigquery_dataset_id: "logs")
    backend = insert(:backend, user: user, type: :clickhouse)
    %{user: user, backend: backend}
  end

  test "query metadata ignores legacy replica misses", %{user: user, backend: backend} do
    Cachex.put!(Users.Cache, {:get, [user.id]}, {:cached, nil})
    Cachex.put!(Backends.Cache, {:get_backend, [backend.id]}, {:cached, nil})
    Cachex.put!(Backends.Cache, {:list_backends, [[user_id: user.id]]}, {:cached, []})

    assert QueryContext.get_user(user.id).id == user.id
    assert QueryContext.get_backend(backend.id).id == backend.id
    assert [cached] = QueryContext.list_backends(user.id)
    assert cached.id == backend.id
  end

  test "fresh SQL reads remain independent of explicit cached consistency", %{user: user} do
    insert(:source, user: user, name: "events")
    assert QueryContext.get_user(user.id).bigquery_project_id == "original-project"
    Repo.update!(Ecto.Changeset.change(user, bigquery_project_id: "updated-project"))

    assert {:ok, cached} =
             Sql.transform(:bq_sql, "SELECT id FROM events", user.id, consistency: :cached)

    assert cached =~ "original-project.logs."
    assert {:ok, fresh} = Sql.transform(:bq_sql, "SELECT id FROM events", user.id)
    assert fresh =~ "updated-project.logs."
  end

  test "application updates invalidate warm user snapshots outside transactions", %{user: user} do
    assert QueryContext.get_user(user.id).bigquery_project_id == "original-project"

    assert {:ok, _} =
             Users.update_user_all_fields(user, %{bigquery_project_id: "updated-project"})

    assert QueryContext.get_user(user.id).bigquery_project_id == "updated-project"
  end

  test "transactions read their writes without publishing uncommitted snapshots", context do
    insert(:source, user: context.user, name: "events")

    assert {:ok, _} =
             Sql.transform(:bq_sql, "SELECT id FROM events", context.user.id,
               consistency: :cached
             )

    assert QueryContext.get_user(context.user.id).bigquery_project_id == "original-project"
    assert QueryContext.get_backend(context.backend.id).name == context.backend.name
    assert [_] = QueryContext.list_backends(context.user.id)

    assert {:error, :rollback} =
             Repo.transaction(fn ->
               updated_user =
                 Repo.update!(
                   Ecto.Changeset.change(context.user, bigquery_project_id: "uncommitted")
                 )

               updated_backend =
                 Repo.update!(Ecto.Changeset.change(context.backend, name: "Uncommitted"))

               extra = insert(:backend, user: context.user, type: :postgres)

               assert QueryContext.get_user(context.user.id).bigquery_project_id ==
                        updated_user.bigquery_project_id

               assert {:ok, sql} =
                        Sql.transform(:bq_sql, "SELECT id FROM events", context.user.id,
                          consistency: :cached
                        )

               assert sql =~ "uncommitted.logs."
               assert QueryContext.get_backend(context.backend.id).name == updated_backend.name

               assert MapSet.new(QueryContext.list_backends(context.user.id), & &1.id) ==
                        MapSet.new([context.backend.id, extra.id])

               Repo.rollback(:rollback)
             end)

    assert QueryContext.get_user(context.user.id).bigquery_project_id == "original-project"
    assert QueryContext.get_backend(context.backend.id).name == context.backend.name
    assert [_] = QueryContext.list_backends(context.user.id)
  end

  test "application user updates invalidate only after the outer transaction commits", %{
    user: user
  } do
    assert QueryContext.get_user(user.id).bigquery_project_id == "original-project"
    before = Cachex.keys!(Cache)

    assert {:ok, :ok} =
             Repo.transaction(fn ->
               assert {:ok, _} =
                        Users.update_user_all_fields(user, %{
                          bigquery_project_id: "updated-project"
                        })

               assert Cachex.keys!(Cache) == before
               assert QueryContext.get_user(user.id).bigquery_project_id == "updated-project"
               :ok
             end)

    assert QueryContext.get_user(user.id).bigquery_project_id == "updated-project"
  end

  test "rolled-back application writes do not invalidate committed snapshots", %{user: user} do
    assert QueryContext.get_user(user.id).bigquery_project_id == "original-project"
    before = Cachex.keys!(Cache)

    assert {:error, :rollback} =
             Repo.transaction(fn ->
               assert {:ok, _} =
                        Users.update_user_all_fields(user, %{bigquery_project_id: "uncommitted"})

               Repo.rollback(:rollback)
             end)

    assert Cachex.keys!(Cache) == before
    assert QueryContext.get_user(user.id).bigquery_project_id == "original-project"
  end

  test "backend mutations invalidate rows and owner lists after commit", %{
    user: user,
    backend: backend
  } do
    assert QueryContext.get_backend(backend.id).name == backend.name
    assert [_] = QueryContext.list_backends(user.id)
    before = Cachex.keys!(Cache)

    assert {:ok, :ok} =
             Repo.transaction(fn ->
               assert {:ok, _} = Backends.update_backend(backend, %{name: "Updated"})
               assert Cachex.keys!(Cache) == before
               :ok
             end)

    assert QueryContext.get_backend(backend.id).name == "Updated"
    assert [%{name: "Updated"}] = QueryContext.list_backends(user.id)

    assert {:ok, created} =
             Backends.create_backend(user, %{
               type: :postgres,
               name: "Other",
               config: %{url: "postgresql://localhost:5432/logflare_test"}
             })

    assert length(QueryContext.list_backends(user.id)) == 2
    assert QueryContext.get_backend(created.id).id == created.id
    assert {:ok, _} = Backends.delete_backend(created)
    assert nil == QueryContext.get_backend(created.id)
    assert [_] = QueryContext.list_backends(user.id)
  end

  test "known-owner invalidation leaves other owners warm", %{user: user, backend: backend} do
    other = insert(:user)
    assert [_] = QueryContext.list_backends(user.id)
    assert [] = QueryContext.list_backends(other.id)
    QueryContext.invalidate_backend(backend)

    reject(Backends, :list_backends, 1)
    assert [] = QueryContext.list_backends(other.id)
  end

  test "late legacy owner-list gossip cannot change query backend selection", %{
    user: user,
    backend: backend
  } do
    other = insert(:backend, user: user, type: :postgres)
    QueryContext.invalidate_backend(backend)
    key = {:list_backends, [[user_id: user.id]]}
    Gossip.receive(Backends.Cache, key, [other])

    assert Enum.any?(QueryContext.list_backends(user.id), &(&1.id == backend.id))
  end

  test "query-context fills never multicast snapshots", %{user: user, backend: backend} do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :context_cache_gossip, :multicast, :stop]
      ])

    on_exit(fn -> :telemetry.detach(ref) end)
    QueryContext.get_user(user.id)
    QueryContext.get_backend(backend.id)
    QueryContext.list_backends(user.id)
    refute_received {_, ^ref, _, _}
  end

  test "failed fills are retryable rather than cached", %{user: user} do
    stub(Users, :get, fn id -> Mimic.call_original(Users, :get, [id]) end)

    expect(Users, :get, fn _ ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    assert {:error, :database_unavailable} = QueryContext.get_user(user.id)
    assert QueryContext.get_user(user.id).id == user.id
  end

  for kind <- [:user, :backend, :owner] do
    @tag kind: kind
    test "invalidation fences an in-flight #{kind} snapshot", %{
      kind: kind,
      user: user,
      backend: backend
    } do
      {key, fields, old, new} =
        case kind do
          :user ->
            {{:get_user, [user.id]}, [user_id: user.id], user, %{user | name: "New"}}

          :backend ->
            {{:get_backend, [backend.id]}, [backend_id: backend.id, owner_id: user.id], nil,
             backend}

          :owner ->
            {{:list_backends, [user.id]}, [backend_id: backend.id], [], [backend]}
        end

      test_pid = self()
      counter = start_supervised!({Agent, fn -> 0 end})

      loader = fn ->
        case Agent.get_and_update(counter, &{&1, &1 + 1}) do
          0 ->
            send(test_pid, {:snapshot_read, self()})

            receive do
              :release -> old
            end

          _ ->
            new
        end
      end

      task = Task.async(fn -> Cache.fetch(key, loader) end)
      assert_receive {:snapshot_read, worker}, 1_000
      assert {:ok, 0} = Cache.invalidate(fields)
      send(worker, :release)
      assert Task.await(task) == new
      assert Cache.fetch(key, fn -> new end) == new
    end
  end
end
