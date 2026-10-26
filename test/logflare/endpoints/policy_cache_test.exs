defmodule Logflare.Endpoints.PolicyCacheTest do
  use Logflare.DataCase

  alias Logflare.Endpoints
  alias Logflare.Endpoints.Cache

  setup do
    endpoint =
      insert(:endpoint,
        language: :ch_sql,
        enforced_clickhouse_settings: %{"max_execution_time" => 7}
      )

    on_exit(fn -> Cache.invalidate_endpoint_policy(endpoint.id) end)
    %{endpoint: endpoint}
  end

  test "misses read the primary and hits do not read the database", %{endpoint: endpoint} do
    expect(Endpoints, :get_endpoint_query, fn id ->
      assert id == endpoint.id
      assert Repo.get_dynamic_repo() == Repo
      endpoint
    end)

    previous = Repo.put_dynamic_repo(:unavailable_replica)

    try do
      assert Cache.get_endpoint_policy(endpoint.id) == endpoint
      assert Repo.get_dynamic_repo() == :unavailable_replica
      reject(Endpoints, :get_endpoint_query, 1)
      assert Cache.get_endpoint_policy(endpoint.id) == endpoint
    after
      Repo.put_dynamic_repo(previous)
    end
  end

  test "cached hits do not wait behind an unrelated primary read", %{endpoint: warm} do
    cached = Cache.get_endpoint_policy(warm.id)
    cold = insert(:endpoint, language: :ch_sql)
    test_pid = self()

    expect(Endpoints, :get_endpoint_query, fn id ->
      assert id == cold.id
      send(test_pid, {:blocked_policy_read, self()})

      receive do
        :finish -> cold
      end
    end)

    reader = Task.async(fn -> Cache.get_endpoint_policy(cold.id) end)
    assert_receive {:blocked_policy_read, getter}
    hit = Task.async(fn -> send(test_pid, {:cached_hit, Cache.get_endpoint_policy(warm.id)}) end)

    try do
      assert_receive {:cached_hit, ^cached}, 500
    after
      send(getter, :finish)
      Task.await(reader)
      Task.await(hit)
    end
  end

  test "recoverable primary failures propagate without caching the error", %{endpoint: endpoint} do
    for failure <- [
          fn -> raise DBConnection.ConnectionError, message: "primary unavailable" end,
          fn -> exit({:noproc, {Repo, :query, []}}) end
        ] do
      stub(Endpoints, :get_endpoint_query, fn _id -> failure.() end)
      assert Cache.get_endpoint_policy(endpoint.id) == {:error, :database_unavailable}
      assert Endpoints.run_query(endpoint) == {:error, :database_unavailable}
      assert Endpoints.get_transformed_query(endpoint) == {:error, :database_unavailable}
      assert Cachex.get(Cache, {:get_endpoint_policy, [endpoint.id]}) == {:ok, nil}
    end

    expect(Endpoints, :get_endpoint_query, fn _id -> endpoint end)
    assert Cache.get_endpoint_policy(endpoint.id) == endpoint
  end

  test "unexpected getter exceptions are not reported as database unavailability", context do
    expect(Endpoints, :get_endpoint_query, fn _id -> raise "unexpected policy bug" end)
    assert_raise MatchError, fn -> Cache.get_endpoint_policy(context.endpoint.id) end
  end

  test "policy reads ignore stale replica-backed endpoint entries", %{endpoint: endpoint} do
    stale = %{endpoint | enforced_clickhouse_settings: %{}}
    Cachex.put(Cache, {:get_endpoint_query, [endpoint.id]}, {:cached, stale})
    Cachex.put(Cache, {:get_mapped_query_by_token, [endpoint.token]}, {:cached, stale})

    assert Cache.get_endpoint_query(endpoint.id).enforced_clickhouse_settings == %{}

    assert Cache.get_endpoint_policy(endpoint.id).enforced_clickhouse_settings == %{
             "max_execution_time" => 7
           }
  end

  test "eviction waits for an old in-flight fill before acknowledging", %{endpoint: endpoint} do
    test_pid = self()

    stub(Endpoints, :get_endpoint_query, fn id ->
      Mimic.call_original(Endpoints, :get_endpoint_query, [id])
    end)

    expect(Endpoints, :get_endpoint_query, fn _id ->
      send(test_pid, {:policy_read_started, self()})

      receive do
        :finish -> endpoint
      end
    end)

    reader = Task.async(fn -> Cache.get_endpoint_policy(endpoint.id) end)
    assert_receive {:policy_read_started, reader_pid}

    updated =
      endpoint
      |> Ecto.Changeset.change(enforced_clickhouse_settings: %{"max_execution_time" => 1})
      |> Repo.update!()

    invalidator = Task.async(fn -> Cache.invalidate_endpoint_policy_cluster(endpoint.id) end)
    assert Task.yield(invalidator, 100) == nil
    send(reader_pid, :finish)

    assert Task.await(reader) == endpoint
    assert Task.await(invalidator) == :ok

    assert Cache.get_endpoint_policy(endpoint.id).enforced_clickhouse_settings ==
             updated.enforced_clickhouse_settings
  end

  test "missing endpoints remain missing instead of using a caller's stale policy" do
    assert Cache.get_endpoint_policy(-1) == nil
    assert :ok = Cache.invalidate_endpoint_policy(-1)
  end
end
