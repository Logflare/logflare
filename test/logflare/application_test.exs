defmodule Logflare.ApplicationTest do
  @moduledoc false
  use ExUnit.Case, async: false

  describe "datadog_connection_pools/0" do
    setup do
      original_config = Application.get_env(:logflare, :http_connection_pools)

      on_exit(fn ->
        if original_config do
          Application.put_env(:logflare, :http_connection_pools, original_config)
        else
          Application.delete_env(:logflare, :http_connection_pools)
        end
      end)
    end

    defp get_pools, do: apply(Logflare.Networking, :datadog_connection_pools, [])

    test "returns all datadog pools for default and 'all' configs" do
      for config <- [nil, ["all"]] do
        if config,
          do: Application.put_env(:logflare, :http_connection_pools, config),
          else: Application.delete_env(:logflare, :http_connection_pools)

        pools = get_pools()
        assert map_size(pools) == 5
        assert Map.has_key?(pools, "https://http-intake.logs.datadoghq.com")
      end
    end

    test "returns datadog pools when 'datadog' is included" do
      for config <- [["datadog"], ["datadog", "elastic"], ["elastic", "all", "loki"]] do
        Application.put_env(:logflare, :http_connection_pools, config)
        pools = get_pools()
        assert map_size(pools) == 5
        assert Map.has_key?(pools, "https://http-intake.logs.datadoghq.com")
      end
    end

    test "returns no pools when datadog not included" do
      for config <- [["elastic"], ["none"], ["elastic", "loki"]] do
        Application.put_env(:logflare, :http_connection_pools, config)
        assert(get_pools() == %{})
      end
    end
  end
end
