defmodule Logflare.Endpoints.EndpointPolicyDistributedTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import Logflare.ClickHouseMappedEvents, only: [build_mapped_log_event: 1]
  import Logflare.DataCase
  import Logflare.Factory
  import Mimic

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints
  alias Logflare.Repo
  alias Logflare.TestUtils

  require Logflare.TestUtils
  alias PaperTrail.Version

  @moduletag :cluster

  setup :set_mimic_global
  setup :verify_on_exit!

  setup_all do
    unless Node.alive?() do
      {_output, 0} = System.cmd(System.find_executable("epmd"), ["-daemon"])
      {:ok, _} = :net_kernel.start(:"endpoint_policy_test@127.0.0.1", %{})
      on_exit(fn -> :net_kernel.stop() end)
    end

    :ok
  end

  setup do
    :ok = Sandbox.mode(Repo, :auto)

    on_exit(fn ->
      ClickHouseAdaptor.QueryConnectionSup.terminate_all()
      Sandbox.mode(Repo, :manual)
    end)

    :ok
  end

  test "operator saves stop remote caches and replacement queries ignore stale remote policy" do
    {:ok, {plan, owner, admin, source, backend, endpoint}} =
      Repo.transaction(fn ->
        plan = insert(:plan)
        owner = insert(:user)
        admin = insert(:user, admin: true)
        {source, backend} = setup_clickhouse_test(user: owner, cleanup?: false)
        table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

        endpoint =
          insert(:endpoint,
            user: owner,
            backend: backend,
            language: :ch_sql,
            query: "SELECT event_message FROM #{table} WHERE source_name = '#{source.name}'",
            enforced_clickhouse_settings: %{"max_rows_to_read" => 100},
            cache_duration_seconds: 60,
            proactive_requerying_seconds: 60
          )

        {plan, owner, admin, source, backend, endpoint}
      end)

    on_exit(fn ->
      Repo.transaction(fn ->
        Repo.delete_all(
          from(v in Version, where: v.item_type == "EndpointQuery" and v.item_id == ^endpoint.id)
        )

        Repo.delete!(endpoint)
        Repo.delete_all(from(sb in "sources_backends", where: sb.backend_id == ^backend.id))
        Repo.delete!(backend)
        Repo.delete!(source)
        Repo.delete!(admin)
        Repo.delete!(owner)
        Repo.delete!(plan)
      end)
    end)

    start_supervised!({ClickHouseAdaptor, backend})
    on_exit(fn -> cleanup_clickhouse_tables(backend) end)
    assert :ok = ClickHouseAdaptor.provision_ingest_tables(backend)

    events =
      for i <- 1..5, do: build_mapped_log_event(source: source, message: "remote policy #{i}")

    assert :ok = ClickHouseAdaptor.insert_log_events(backend, events, :log)
    table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

    TestUtils.retry_assert(fn ->
      assert {:ok, {[%{"total" => 5}], _}} =
               ClickHouseAdaptor.execute_ch_query(
                 backend,
                 "SELECT count() AS total FROM #{table}"
               )
    end)

    peer_node = start_peer()
    assert :erpc.call(peer_node, System, :schedulers_online, []) != System.schedulers_online()

    assert {:ok, %{rows: rows}} =
             :erpc.call(peer_node, Endpoints, :run_cached_query, [endpoint])

    assert length(rows) == 5

    assert %{enforced_clickhouse_settings: %{"max_rows_to_read" => 100}} =
             :erpc.call(peer_node, Endpoints.Cache, :get_endpoint_policy, [endpoint.id])

    [cache] = :erpc.call(peer_node, Endpoints.Resolver, :list_caches, [endpoint, :all_versions])
    assert node(cache) == peer_node
    monitor = Process.monitor(cache)

    TestUtils.retry_assert(fn ->
      assert cache in Endpoints.Resolver.list_caches(endpoint, :all_versions)
    end)

    assert {:ok, _} =
             Endpoints.configure_enforced_clickhouse_settings(admin, endpoint, %{
               "max_rows_to_read" => 1
             })

    assert_receive {:DOWN, ^monitor, :process, ^cache, :normal}, 5_000

    assert {:ok, nil} =
             :erpc.call(peer_node, Cachex, :get, [
               Endpoints.Cache,
               {:get_endpoint_policy, [endpoint.id]}
             ])

    assert {:ok, true} =
             :erpc.call(peer_node, Cachex, :put, [
               Endpoints.Cache,
               {:get_mapped_query_by_token, [endpoint.token]},
               {:cached, endpoint}
             ])

    assert %{} =
             stale =
             :erpc.call(peer_node, Endpoints.Cache, :get_mapped_query_by_token, [endpoint.token])

    assert stale.enforced_clickhouse_settings == %{"max_rows_to_read" => 100}

    assert {:error, %QueryError{} = error} =
             :erpc.call(peer_node, Endpoints, :run_cached_query, [endpoint])

    assert inspect(error.raw_error) =~ "max_rows_to_read"

    expect(Endpoints.Resolver, :resolve, fn _, _, _ -> cache end)
    assert {:error, %QueryError{} = retried_error} = Endpoints.run_cached_query(endpoint)
    assert inspect(retried_error.raw_error) =~ "max_rows_to_read"

    refute cache in :erpc.call(peer_node, Endpoints.Resolver, :list_caches, [
             endpoint,
             :all_versions
           ])
  end

  defp start_peer do
    schedulers = if System.schedulers_online() == 2, do: 1, else: 2

    {:ok, peer, peer_node} =
      :peer.start(%{
        name: :endpoint_policy_peer,
        host: ~c"127.0.0.1",
        args: [~c"+S", String.to_charlist("#{schedulers}:#{schedulers}")],
        env: [{~c"ERL_AFLAGS", ~c"-setcookie #{:erlang.get_cookie()}"}]
      })

    on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])

    for {app, _, _} <- Application.loaded_applications(),
        {key, value} <- Application.get_all_env(app) do
      :erpc.call(peer_node, Application, :put_env, [app, key, value, [persistent: true]])
    end

    :erpc.call(peer_node, Application, :put_env, [:logflare, :enable_cainophile, false])

    :erpc.call(peer_node, Application, :put_env, [
      :logflare,
      :context_cache_gossip,
      %{enabled: false}
    ])

    {:ok, _apps} = :erpc.call(peer_node, Application, :ensure_all_started, [:logflare])
    :ok = :erpc.call(peer_node, Sandbox, :mode, [Repo, :auto])
    peer_node
  end
end
