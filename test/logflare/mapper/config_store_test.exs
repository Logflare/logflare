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

  describe "get_compiled/2" do
    test "returns the eagerly compiled RowBinary mapping for each event type" do
      for event_type <- [:log, :metric, :trace] do
        assert {:ok, ref, config_id} = ConfigStore.get_compiled(event_type, :ch_row_binary)
        assert is_reference(ref)
        assert config_id == OtelDefaults.config_id(event_type)
      end
    end

    test "raises for unknown event type" do
      assert_raise FunctionClauseError, fn ->
        ConfigStore.get_compiled(:unknown, :ch_row_binary)
      end
    end

    test "compiles other formats lazily and caches them" do
      for event_type <- [:log, :metric, :trace], format <- [:ndjson, :map] do
        assert {:ok, ref, config_id} = ConfigStore.get_compiled(event_type, format)
        assert is_reference(ref)
        assert config_id == OtelDefaults.config_id(event_type)

        assert [{{^event_type, ^format}, ^ref, ^config_id}] =
                 :ets.lookup(:mapper_config_store, {event_type, format})

        assert {:ok, ^ref, ^config_id} = ConfigStore.get_compiled(event_type, format)
      end
    end

    test "returns distinct references per format sharing one config_id" do
      {:ok, rowbinary, id} = ConfigStore.get_compiled(:log, :ch_row_binary)
      {:ok, ndjson, ^id} = ConfigStore.get_compiled(:log, :ndjson)
      {:ok, map, ^id} = ConfigStore.get_compiled(:log, :map)

      assert rowbinary != ndjson
      assert ndjson != map
    end

    test "raises for unknown output format" do
      assert_raise FunctionClauseError, fn ->
        ConfigStore.get_compiled(:log, :csv)
      end
    end
  end
end
