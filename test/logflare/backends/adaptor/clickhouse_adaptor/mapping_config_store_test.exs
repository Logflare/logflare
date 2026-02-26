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

  describe "get_compiled/2 with :simple variant" do
    test "returns {:ok, reference, config_id} for :log" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:log, :simple)
      assert is_reference(ref)
      assert config_id == MappingDefaults.config_id_simple(:log)
    end

    test "returns {:ok, reference, config_id} for :metric" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:metric, :simple)
      assert is_reference(ref)
      assert config_id == MappingDefaults.config_id_simple(:metric)
    end

    test "returns {:ok, reference, config_id} for :trace" do
      assert {:ok, ref, config_id} = MappingConfigStore.get_compiled(:trace, :simple)
      assert is_reference(ref)
      assert config_id == MappingDefaults.config_id_simple(:trace)
    end

    test "returns different config_ids than standard variants" do
      {:ok, _, standard_id} = MappingConfigStore.get_compiled(:log)
      {:ok, _, simple_id} = MappingConfigStore.get_compiled(:log, :simple)
      assert standard_id != simple_id
    end

    test "returns different compiled references than standard variants" do
      {:ok, standard_ref, _} = MappingConfigStore.get_compiled(:log)
      {:ok, simple_ref, _} = MappingConfigStore.get_compiled(:log, :simple)
      assert standard_ref != simple_ref
    end

    test "nil variant returns standard config" do
      {:ok, ref_default, id_default} = MappingConfigStore.get_compiled(:log)
      {:ok, ref_nil, id_nil} = MappingConfigStore.get_compiled(:log, nil)
      assert ref_default == ref_nil
      assert id_default == id_nil
    end

    test "raises for unknown event type with :simple" do
      assert_raise FunctionClauseError, fn ->
        MappingConfigStore.get_compiled(:unknown, :simple)
      end
    end
  end
end
