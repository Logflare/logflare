defmodule Logflare.ContextCache.CatalogWalTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Alerting
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sources.Catalog

  setup do
    insert(:plan)

    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    %{user: insert(:user)}
  end

  for {context, cache, getter, factory, table, loader} <- [
        {Sources, Catalog.Cache, :list_by_user, :source, "sources", :list_source_catalog_by_user},
        {Endpoints, Endpoints.Cache, :list_by_user_id, :endpoint, "endpoint_queries",
         :list_endpoints_by},
        {Alerting, Alerting.Cache, :list_by_user_id, :alert, "alert_queries",
         :list_alert_queries_by_user_id}
      ] do
    @context context
    @cache cache
    @getter getter
    @factory factory
    @table table
    @loader loader

    test "#{table} WAL retains owner IDs on deletes and transfers" do
      assert %{rows: [["f"]]} =
               Repo.query!(
                 "SELECT relreplident::text FROM pg_class WHERE oid = to_regclass($1)",
                 [
                   @table
                 ]
               )
    end

    test "#{table} transfers invalidate the old and destination owner catalogs", %{user: owner} do
      destination = insert(:user)
      moved = insert(@factory, user: owner)
      insert(@factory, user: destination)
      assert [_row] = apply(@cache, @getter, [owner.id])
      assert [_row] = apply(@cache, @getter, [destination.id])
      moved |> Ecto.Changeset.change(user_id: destination.id) |> Repo.update!()

      records =
        dispatch(%UpdatedRecord{
          relation: {"public", @table},
          record: %{"id" => to_string(moved.id), "user_id" => to_string(destination.id)},
          old_record: %{"id" => to_string(moved.id), "user_id" => to_string(owner.id)}
        })

      catalog_context = if @context == Sources, do: Catalog, else: @context
      assert {catalog_context, [user_id: owner.id]} in records
      assert {catalog_context, [user_id: destination.id]} in records
      assert [] = apply(@cache, @getter, [owner.id])
      assert length(apply(@cache, @getter, [destination.id])) == 2
    end

    for operation <- [:update, :delete] do
      @operation operation

      test "#{table} #{@operation} WAL fences an in-flight catalog fill", %{user: user} do
        row = insert(@factory, user: user, name: "original")
        test_pid = self()
        counter = start_supervised!({Agent, fn -> 0 end})

        stub(@context, @loader, fn argument ->
          case Agent.get_and_update(counter, &{&1, &1 + 1}) do
            0 ->
              send(test_pid, {:snapshot_read, self()})

              receive do
                :release -> [row]
              end

            _next_read ->
              assert Repo.get_dynamic_repo() == Repo
              Mimic.call_original(@context, @loader, [argument])
          end
        end)

        task = Task.async(fn -> apply(@cache, @getter, [user.id]) end)
        assert_receive {:snapshot_read, worker}, 1_000
        dispatch(change_record(@operation, @table, row))
        send(worker, :release)
        result = Task.await(task)

        case @operation do
          :update -> assert [%{name: "updated"}] = result
          :delete -> assert [] = result
        end

        assert apply(@cache, @getter, [user.id]) == result
      end
    end
  end

  defp change_record(:update, table, row) do
    row |> Ecto.Changeset.change(name: "updated") |> Repo.update!()
    record = %{"id" => to_string(row.id), "user_id" => to_string(row.user_id)}
    %UpdatedRecord{relation: {"public", table}, record: record, old_record: record}
  end

  defp change_record(:delete, table, row) do
    Repo.delete!(row)

    %DeletedRecord{
      relation: {"public", table},
      old_record: %{"id" => to_string(row.id), "user_id" => to_string(row.user_id)}
    }
  end

  defp dispatch(change) do
    test_pid = self()

    expect(ContextCache, :bust_keys, fn records ->
      result = Mimic.call_original(ContextCache, :bust_keys, [records])
      send(test_pid, {:busted, records})
      result
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive {:busted, records}, 1_000
    records
  end
end
