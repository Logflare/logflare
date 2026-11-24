defmodule Logflare.Endpoints.QueryNameCacheTest do
  use Logflare.DataCase

  alias Logflare.Alerting
  alias Logflare.ContextCache
  alias Logflare.Endpoints

  setup do
    %{user: insert(:user)}
  end

  test "the compact index contains only this owner's names, ids, kinds and languages", %{
    user: user
  } do
    endpoint = insert(:endpoint, user: user, language: :ch_sql)
    alert = insert(:alert, user: user, language: :pg_sql)
    insert(:endpoint)
    other_user = insert(:user)
    insert(:alert, user: other_user)

    assert Enum.sort_by(Endpoints.Cache.list_query_names(user.id), & &1.kind) == [
             %{id: alert.id, name: alert.name, language: :pg_sql, kind: "alert"},
             %{id: endpoint.id, name: endpoint.name, language: :ch_sql, kind: "endpoint"}
           ]

    reject(Endpoints, :list_query_names, 1)
    assert length(Endpoints.Cache.list_query_names(user.id)) == 2
  end

  for {factory, context} <- [endpoint: Endpoints, alert: Alerting] do
    @tag factory: factory, context: context
    test "#{factory} owner invalidation refreshes a cached empty index", context do
      assert [] = Endpoints.Cache.list_query_names(context.user.id)
      query = insert(context.factory, user: context.user)
      assert [] = Endpoints.Cache.list_query_names(context.user.id)

      assert {:ok, _} =
               ContextCache.bust_keys([{context.context, user_id: context.user.id}])

      assert [%{id: id}] = Endpoints.Cache.list_query_names(context.user.id)
      assert id == query.id
    end

    @tag factory: factory, context: context
    test "#{factory} id invalidation refreshes renamed and deleted names", context do
      query = insert(context.factory, user: context.user)
      assert [%{name: original}] = Endpoints.Cache.list_query_names(context.user.id)
      updated = Repo.update!(Ecto.Changeset.change(query, name: "renamed"))
      assert [%{name: ^original}] = Endpoints.Cache.list_query_names(context.user.id)

      assert {:ok, _} = ContextCache.bust_keys([{context.context, query.id}])
      assert [%{name: "renamed"}] = Endpoints.Cache.list_query_names(context.user.id)

      Repo.delete!(updated)
      assert {:ok, _} = ContextCache.bust_keys([{context.context, query.id}])
      assert [] = Endpoints.Cache.list_query_names(context.user.id)
    end

    @tag factory: factory, context: context
    test "#{factory} owner invalidation fences an in-flight name-index fill", context do
      query = insert(context.factory, user: context.user)
      original = Endpoints.list_query_names(context.user.id)
      refreshed = [%{hd(original) | name: "renamed"}]
      test_pid = self()

      expect(Endpoints, :list_query_names, fn _ ->
        send(test_pid, {:loaded, self()})

        receive do
          :finish -> original
        end
      end)

      expect(Endpoints, :list_query_names, fn _ -> refreshed end)
      task = Task.async(fn -> Endpoints.Cache.list_query_names(context.user.id) end)
      assert_receive {:loaded, loader}

      Repo.update!(Ecto.Changeset.change(query, name: "renamed"))

      assert {:ok, _} =
               ContextCache.bust_keys([{context.context, user_id: context.user.id}])

      send(loader, :finish)
      assert Task.await(task) == refreshed
    end
  end

  test "owner invalidation leaves another owner's index cached", %{user: user} do
    other_user = insert(:user)
    insert(:endpoint, user: other_user)
    assert [] = Endpoints.Cache.list_query_names(user.id)
    assert [_] = Endpoints.Cache.list_query_names(other_user.id)

    ContextCache.bust_keys([{Alerting, user_id: user.id}])
    reject(Endpoints, :list_query_names, 1)
    assert [_] = Endpoints.Cache.list_query_names(other_user.id)
  end

  test "transaction reads bypass the index without publishing uncommitted names", %{user: user} do
    endpoint = insert(:endpoint, user: user, name: "original")
    assert [%{name: "original"}] = Endpoints.Cache.list_query_names(user.id)

    Repo.transaction(fn ->
      Repo.update!(Ecto.Changeset.change(endpoint, name: "uncommitted"))
      assert [%{name: "uncommitted"}] = Endpoints.Cache.list_query_names(user.id)
      Repo.rollback(:probe)
    end)

    assert [%{name: "original"}] = Endpoints.Cache.list_query_names(user.id)
  end

  test "the index keeps the existing two-minute endpoint-cache TTL", %{user: user} do
    assert [] = Endpoints.Cache.list_query_names(user.id)

    [{:entry, key, _, _, _}] =
      Endpoints.Cache
      |> Cachex.stream!()
      |> Enum.filter(fn {:entry, key, _, _, _} ->
        match?({:catalog, {:list_query_names, [id]}, _} when id == user.id, key)
      end)

    assert ttl = Cachex.ttl!(Endpoints.Cache, key)
    assert ttl > 0
    assert ttl <= to_timeout(minute: 2)
  end
end
