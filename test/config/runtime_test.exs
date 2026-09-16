defmodule Logflare.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias Logflare.SingleTenant

  @backend_env_vars [
    "GOOGLE_SERVICE_ACCOUNT",
    "POSTGRES_BACKEND_URL",
    "CLICKHOUSE_BACKEND_URL"
  ]
  @clickhouse_backend_url "http://localhost:8123/logflare"
  @postgres_backend_url "postgresql://postgres:postgres@localhost/logflare"

  setup do
    previous_values =
      ["LOGFLARE_SINGLE_TENANT" | @backend_env_vars]
      |> Map.new(&{&1, System.get_env(&1)})

    previous_backend = Application.fetch_env(:logflare, :single_tenant_backend)

    System.put_env("LOGFLARE_SINGLE_TENANT", "true")
    Enum.each(@backend_env_vars, &System.delete_env/1)

    on_exit(fn ->
      case previous_backend do
        {:ok, backend} -> Application.put_env(:logflare, :single_tenant_backend, backend)
        :error -> Application.delete_env(:logflare, :single_tenant_backend)
      end

      Enum.each(previous_values, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)
  end

  test "defaults the single-tenant backend to BigQuery when no backend URL is configured" do
    assert runtime_backend_type() == :bigquery
  end

  test "selects the configured backend" do
    for {env, value, expected_backend} <- [
          {"GOOGLE_SERVICE_ACCOUNT", "service-account", :bigquery},
          {"POSTGRES_BACKEND_URL", @postgres_backend_url, :postgres},
          {"CLICKHOUSE_BACKEND_URL", @clickhouse_backend_url, :clickhouse}
        ] do
      System.put_env(env, value)

      assert runtime_backend_type() == expected_backend

      System.delete_env(env)
    end
  end

  test "ignores blank backend URLs" do
    System.put_env("POSTGRES_BACKEND_URL", " ")
    System.put_env("CLICKHOUSE_BACKEND_URL", "\t")

    assert runtime_backend_type() == :bigquery
  end

  test "rejects multiple backend URL configurations" do
    System.put_env("POSTGRES_BACKEND_URL", @postgres_backend_url)
    System.put_env("CLICKHOUSE_BACKEND_URL", @clickhouse_backend_url)

    assert_raise RuntimeError,
                 "Only one of GOOGLE_SERVICE_ACCOUNT, POSTGRES_BACKEND_URL, or CLICKHOUSE_BACKEND_URL may be configured",
                 &runtime_config/0
  end

  test "rejects BigQuery with another backend configuration" do
    System.put_env("GOOGLE_SERVICE_ACCOUNT", "service-account")

    for {backend_env, value} <- [
          {"POSTGRES_BACKEND_URL", @postgres_backend_url},
          {"CLICKHOUSE_BACKEND_URL", @clickhouse_backend_url}
        ] do
      System.put_env(backend_env, value)

      assert_raise RuntimeError,
                   "Only one of GOOGLE_SERVICE_ACCOUNT, POSTGRES_BACKEND_URL, or CLICKHOUSE_BACKEND_URL may be configured",
                   &runtime_config/0

      System.delete_env(backend_env)
    end
  end

  test "defaults to BigQuery outside single-tenant mode" do
    System.put_env("LOGFLARE_SINGLE_TENANT", "false")

    for {configured_env, value, unconfigured_env} <- [
          {"POSTGRES_BACKEND_URL", @postgres_backend_url, "CLICKHOUSE_BACKEND_URL"},
          {"CLICKHOUSE_BACKEND_URL", @clickhouse_backend_url, "POSTGRES_BACKEND_URL"}
        ] do
      System.put_env(configured_env, value)
      System.delete_env(unconfigured_env)

      assert runtime_backend_type() == :bigquery
    end
  end

  defp runtime_backend_type do
    backend_type = get_in(runtime_config(), [:logflare, :single_tenant_backend])
    Application.put_env(:logflare, :single_tenant_backend, backend_type)
    SingleTenant.backend_type()
  end

  defp runtime_config do
    Config.Reader.read!("config/runtime.exs", env: :test)
  end
end
