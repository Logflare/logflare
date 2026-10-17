defmodule Logflare.Backends.SystemBackendTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.SystemBackend
  alias Logflare.Sources.Source

  TestUtils.setup_single_tenant(backend_type: :bigquery)

  test "dispatches lifecycle hooks to the single-tenant BigQuery adaptor" do
    assert_bigquery_hooks_dispatched(build(:source))
  end

  test "multi-tenant deployments use BigQuery regardless of the single-tenant selection" do
    Application.put_env(:logflare, :single_tenant, false)
    Application.put_env(:logflare, :single_tenant_backend, :clickhouse)
    assert_bigquery_hooks_dispatched(build(:source))
  end

  test "PostgreSQL and ClickHouse do not run BigQuery setup" do
    reject(BigQueryAdaptor, :on_system_start, 0)
    reject(BigQueryAdaptor, :on_source_start, 1)
    reject(BigQueryAdaptor, :on_supabase_start, 0)

    for type <- [:postgres, :clickhouse] do
      Application.put_env(:logflare, :single_tenant_backend, type)

      assert :ok = SystemBackend.on_system_start()
      assert :ok = SystemBackend.on_source_start(build(:source))
      assert :ok = SystemBackend.on_supabase_start()
    end
  end

  @spec assert_bigquery_hooks_dispatched(Source.t()) :: :ok
  defp assert_bigquery_hooks_dispatched(source) do
    expect(BigQueryAdaptor, :on_system_start, fn -> :ok end)
    expect(BigQueryAdaptor, :on_source_start, fn ^source -> :ok end)
    expect(BigQueryAdaptor, :on_supabase_start, fn -> :ok end)

    assert :ok = SystemBackend.on_system_start()
    assert :ok = SystemBackend.on_source_start(source)
    assert :ok = SystemBackend.on_supabase_start()
  end
end
