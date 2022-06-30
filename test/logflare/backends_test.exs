defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{Backends, Backends.SourceBackend}

  test "can attach multiple backends to a source", %{source: source} do
    assert {:ok, %SourceBackend{}} = Backends.create_source_backend(source)
    assert {:ok, %SourceBackend{}} = Backends.create_source_backend(source, %{type: :webhook})

    # cannot attach multiple instances of the same backend type to the source
    assert {:error, %Ecto.Changeset{}} = Backends.create_source_backend(source, %{type: :webhook})
  end


  describe ("dispatch_ingest") do
    setup do
      source_backend = insert(:source_backend)
      {:ok, source_backend: source_backend}
    end
    test "dispatch_ingest", %{source_backend: source_backend} do
      # send the log event through rules
      log_event = %LogEvent{}
      assert :ok = Backends.dispatch_ingest(source_backend,[log_event])

      raise "not impl"
    end
  end
  describe ("dispatch_execute_query") do
    setup do
      source_backend = insert(:source_backend)
      {:ok, source_backend: source_backend}
    end
    test "dispatch_ingest", %{source_backend: source_backend} do
      # send the log event through rules
      log_event = %LogEvent{}
      assert :ok = Backends.dispatch_ingest(source_backend,[log_event])

      raise "not impl"
    end
  end

end
