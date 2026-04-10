defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStoreTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults

  setup_all do
    case MappingConfigStore.start_link([]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  describe "get_compiled/1" do
    test "returns {:ok, reference, config_id} for :log" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:log)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "returns {:ok, reference, config_id} for :metric" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:metric)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "returns {:ok, reference, config_id} for :trace" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:trace)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "raises for unknown log type" do
      assert_raise FunctionClauseError, fn ->
        MappingConfigStore.get_compiled(:unknown)
      end
    end
  end

  describe "config_id values" do
    test "returns correct config_ids for each event type" do
      {:ok, _, log_id} = MappingConfigStore.get_compiled(:log)
      {:ok, _, metric_id} = MappingConfigStore.get_compiled(:metric)
      {:ok, _, trace_id} = MappingConfigStore.get_compiled(:trace)

      assert log_id == MappingDefaults.config_id(:log)
      assert metric_id == MappingDefaults.config_id(:metric)
      assert trace_id == MappingDefaults.config_id(:trace)
    end
  end
end
