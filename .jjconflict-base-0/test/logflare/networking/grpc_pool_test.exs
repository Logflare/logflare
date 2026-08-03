defmodule Logflare.Networking.GrpcPoolTest do
  # async: false required because GRPC.Stub stubs need set_mimic_global
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Networking.GrpcPool

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    # Use a unique name per test to avoid persistent_term and Registry clashes
    name = :"GrpcPool#{System.unique_integer()}"
    channel = %GRPC.Channel{}
    # Default: monitors fail to connect so they don't interfere with channel registration tests
    stub(GRPC.Stub, :connect, fn _url, _opts -> {:error, :econnrefused} end)
    %{name: name, channel: channel}
  end

  defp start_pool(name, opts \\ []) do
    url = Keyword.get(opts, :url, "https://example.com")
    size = Keyword.get(opts, :size, 2)
    start_supervised!({GrpcPool, name: name, url: url, size: size})
  end

  defp registry_name(name), do: Module.concat(name, Registry)

  describe "get_channel/1" do
    test "returns {:error, :not_connected} when no channels registered", %{name: name} do
      start_pool(name)

      assert {:error, :not_connected} = GrpcPool.get_channel(name)
    end

    test "returns {:ok, channel} when a channel is registered", %{name: name, channel: channel} do
      start_pool(name)

      registry = registry_name(name)
      Registry.register(registry, 0, channel)

      assert {:ok, ^channel} = GrpcPool.get_channel(name)
    end

    test "round-robins across registered channels", %{name: name} do
      start_pool(name, size: 2)

      registry = registry_name(name)
      channel_0 = %GRPC.Channel{host: "host-0"}
      channel_1 = %GRPC.Channel{host: "host-1"}

      # Manually register channels under idx 0 and 1
      {:ok, reg_0} = Registry.register(registry, 0, channel_0)
      {:ok, reg_1} = Registry.register(registry, 1, channel_1)

      assert reg_0 == reg_1

      results = for _ <- 1..4, do: GrpcPool.get_channel(name)

      channels = Enum.map(results, fn {:ok, ch} -> ch end)

      # Should cycle: idx 0, 1, 0, 1 (exact order depends on atomic counter starting point)
      channel_0_count = Enum.count(channels, &(&1 == channel_0))
      channel_1_count = Enum.count(channels, &(&1 == channel_1))

      assert channel_0_count == 2
      assert channel_1_count == 2
    end

    test "returns :not_connected for idx with no registered channel in a partial pool", %{
      name: name
    } do
      start_pool(name, size: 2)

      registry = registry_name(name)
      channel_0 = %GRPC.Channel{host: "host-0"}
      Registry.register(registry, 0, channel_0)

      # Only idx 0 is registered; idx 1 is not
      # Collect results across multiple calls — some will be :not_connected
      results = for _ <- 1..4, do: GrpcPool.get_channel(name)

      assert Enum.any?(results, &match?({:ok, _}, &1))
      assert Enum.any?(results, &match?({:error, :not_connected}, &1))
    end
  end

  describe "pool supervision" do
    test "starts registry and monitor children", %{name: name} do
      start_pool(name, size: 2)

      # Registry should be running
      registry = registry_name(name)
      assert Process.whereis(registry) != nil
    end

    test "starts the configured number of monitor workers", %{name: name} do
      start_supervised!({GrpcPool, name: name, url: "https://example.com", size: 3})

      children =
        name
        |> Supervisor.which_children()
        |> Enum.reject(fn {id, _, _, _} -> id == registry_name(name) end)

      assert length(children) == 3
    end
  end
end
