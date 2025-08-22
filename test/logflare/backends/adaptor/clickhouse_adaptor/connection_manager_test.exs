defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager

  setup do
    insert(:plan, name: "Free")

    {source, backend, ch_cleanup_fn} = setup_clickhouse_test()
    on_exit(ch_cleanup_fn)

    ingest_opts = build_clickhouse_connection_opts(source, backend, :ingest)
    query_opts = build_clickhouse_connection_opts(source, backend, :query)

    [
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    ]
  end

  describe "child_spec/1" do
    setup context do
      context
    end

    test "generates correct child spec for `{Source, Backend, ch_opts}`", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts
    } do
      child_spec = ConnectionManager.child_spec({source, backend, ingest_opts})

      assert %{
               id: {ConnectionManager, {source_id, backend_id}},
               start: {ConnectionManager, :start_link, [{^source, ^backend, ^ingest_opts}]}
             } = child_spec

      assert source_id == source.id
      assert backend_id == backend.id
    end

    test "generates correct child spec for `{Backend, ch_opts}`", %{
      backend: backend,
      query_opts: query_opts
    } do
      child_spec = ConnectionManager.child_spec({backend, query_opts})

      assert %{
               id: {ConnectionManager, {backend_id}},
               start: {ConnectionManager, :start_link, [{^backend, ^query_opts}]}
             } = child_spec

      assert backend_id == backend.id
    end

    test "child specs have unique IDs for different sources with same backend", %{
      backend: backend,
      ingest_opts: ingest_opts
    } do
      user = insert(:user)
      source1 = insert(:source, user: user)
      source2 = insert(:source, user: user)

      child_spec1 = ConnectionManager.child_spec({source1, backend, ingest_opts})
      child_spec2 = ConnectionManager.child_spec({source2, backend, ingest_opts})

      assert child_spec1.id != child_spec2.id
      assert child_spec1.id == {ConnectionManager, {source1.id, backend.id}}
      assert child_spec2.id == {ConnectionManager, {source2.id, backend.id}}
    end

    test "child specs have unique IDs for different backends", %{
      source: source,
      ingest_opts: ingest_opts
    } do
      backend1 = insert(:backend, type: :clickhouse)
      backend2 = insert(:backend, type: :clickhouse)

      child_spec1 = ConnectionManager.child_spec({source, backend1, ingest_opts})
      child_spec2 = ConnectionManager.child_spec({source, backend2, ingest_opts})

      assert child_spec1.id != child_spec2.id
      assert child_spec1.id == {ConnectionManager, {source.id, backend1.id}}
      assert child_spec2.id == {ConnectionManager, {source.id, backend2.id}}
    end

    test "child specs work with `Supervisor.start_link`", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts
    } do
      child_spec = ConnectionManager.child_spec({source, backend, ingest_opts})

      {:ok, sup} = Supervisor.start_link([child_spec], strategy: :one_for_one)

      children = Supervisor.which_children(sup)
      assert length(children) == 1

      [{child_id, child_pid, :worker, [ConnectionManager]}] = children
      assert child_id == {ConnectionManager, {source.id, backend.id}}
      assert Process.alive?(child_pid)

      Supervisor.stop(sup)
    end
  end

  describe "connection manager lifecycle" do
    test "starts successfully when provided a source and backend", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts
    } do
      {:ok, manager_pid} = ConnectionManager.start_link({source, backend, ingest_opts})

      assert Process.alive?(manager_pid)
      refute ConnectionManager.pool_active?({source, backend})
    end

    test "starts successfully when provided just a backend", %{
      backend: backend,
      ingest_opts: ingest_opts
    } do
      {:ok, manager_pid} = ConnectionManager.start_link({backend, ingest_opts})

      assert Process.alive?(manager_pid)
      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "ingest pool connection management" do
    setup context do
      {:ok, manager_pid} =
        ConnectionManager.start_link({context.source, context.backend, context.ingest_opts})

      assert Process.alive?(manager_pid)

      context
    end

    test "`ensure_pool_started` starts a new ingest pool", %{source: source, backend: backend} do
      refute ConnectionManager.pool_active?({source, backend})
      assert :ok == ConnectionManager.ensure_pool_started({source, backend})
      assert ConnectionManager.pool_active?({source, backend})
    end

    test "`ensure_pool_started` returns existing connection if already started", %{
      source: source,
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_pool_started({source, backend})
      assert :ok == ConnectionManager.ensure_pool_started({source, backend})
      assert ConnectionManager.pool_active?({source, backend})
    end

    test "`pool_active?` returns false for non-existent connection pools", %{
      source: source,
      backend: backend
    } do
      refute ConnectionManager.pool_active?({source, backend})
    end
  end

  describe "query pool connection management" do
    setup context do
      {:ok, _manager_pid} =
        ConnectionManager.start_link({context.backend, context.ingest_opts})

      context
    end

    test "`ensure_pool_started` starts a new query pool", %{backend: backend} do
      refute ConnectionManager.pool_active?(backend)
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)
    end

    test "`ensure_pool_started` returns existing connection if already started", %{
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)
    end

    test "`pool_active?` returns false for non-existent connection pools", %{
      backend: backend
    } do
      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "ingest activity tracking" do
    setup context do
      {:ok, _manager_pid} =
        ConnectionManager.start_link({context.source, context.backend, context.ingest_opts})

      context
    end

    test "`notify_activity` updates activity timestamp", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.notify_activity({source, backend})
    end
  end

  describe "query activity tracking" do
    setup context do
      {:ok, _manager_pid} =
        ConnectionManager.start_link({context.backend, context.query_opts})

      context
    end

    test "`notify_activity` updates activity timestamp", %{backend: backend} do
      assert :ok == ConnectionManager.notify_activity(backend)
    end
  end

  describe "ingest pool error handling" do
    setup do
      {source, invalid_backend, cleanup_fn} =
        setup_clickhouse_test(
          config: %{
            url: "http://invalid-hostname:8123",
            username: "invalid_user",
            password: "invalid_pass",
            port: 9999
          }
        )

      on_exit(cleanup_fn)

      [source: source, invalid_backend: invalid_backend]
    end

    test "handles invalid database configuration", %{
      source: source,
      invalid_backend: invalid_backend
    } do
      invalid_ingest_opts = build_clickhouse_connection_opts(source, invalid_backend, :ingest)

      invalid_ingest_opts =
        Keyword.merge(invalid_ingest_opts,
          hostname: "invalid-hostname",
          port: 9999,
          username: "invalid_user",
          password: "invalid_pass"
        )

      {:ok, _manager_pid} =
        ConnectionManager.start_link({source, invalid_backend, invalid_ingest_opts})

      case ConnectionManager.ensure_pool_started({source, invalid_backend}) do
        :ok ->
          assert {:error, _reason} = ClickhouseAdaptor.test_connection({source, invalid_backend})

        {:error, _reason} ->
          refute ConnectionManager.pool_active?({source, invalid_backend})
      end
    end
  end

  describe "query pool error handling" do
    setup do
      {source, invalid_backend, cleanup_fn} =
        setup_clickhouse_test(
          config: %{
            url: "http://invalid-hostname:8123",
            username: "invalid_user",
            password: "invalid_pass",
            port: 9999
          }
        )

      on_exit(cleanup_fn)

      [source: source, invalid_backend: invalid_backend]
    end

    test "handles invalid database configuration", %{
      source: source,
      invalid_backend: invalid_backend
    } do
      invalid_query_opts = build_clickhouse_connection_opts(source, invalid_backend, :query)

      invalid_query_opts =
        Keyword.merge(invalid_query_opts,
          hostname: "invalid-hostname",
          port: 9999,
          username: "invalid_user",
          password: "invalid_pass"
        )

      {:ok, _manager_pid} =
        ConnectionManager.start_link({invalid_backend, invalid_query_opts})

      case ConnectionManager.ensure_pool_started(invalid_backend) do
        :ok ->
          assert {:error, _reason} = ClickhouseAdaptor.test_connection(invalid_backend)

        {:error, _reason} ->
          refute ConnectionManager.pool_active?(invalid_backend)
      end
    end
  end
end
