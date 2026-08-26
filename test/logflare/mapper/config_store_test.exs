defmodule Logflare.Mapper.ConfigStoreTest do
  use ExUnit.Case, async: true

  alias Logflare.Mapper.ConfigStore
  alias Logflare.Mapper.OtelDefaults

  setup_all do
    case ConfigStore.start_link([]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  describe "get_compiled/1" do
    test "returns {:ok, reference, config_id} for :log" do
      assert {:ok, ref, config_id} = ConfigStore.get_compiled(:log)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "returns {:ok, reference, config_id} for :metric" do
      assert {:ok, ref, config_id} = ConfigStore.get_compiled(:metric)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "returns {:ok, reference, config_id} for :trace" do
      assert {:ok, ref, config_id} = ConfigStore.get_compiled(:trace)
      assert is_reference(ref)
      assert is_binary(config_id)
    end

    test "raises for unknown log type" do
      assert_raise FunctionClauseError, fn ->
        ConfigStore.get_compiled(:unknown)
      end
    end
  end

  describe "config_id values" do
    test "returns correct config_ids for each event type" do
      {:ok, _, log_id} = ConfigStore.get_compiled(:log)
      {:ok, _, metric_id} = ConfigStore.get_compiled(:metric)
      {:ok, _, trace_id} = ConfigStore.get_compiled(:trace)

      assert log_id == OtelDefaults.config_id(:log)
      assert metric_id == OtelDefaults.config_id(:metric)
      assert trace_id == OtelDefaults.config_id(:trace)
    end
  end
end
