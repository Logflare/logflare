defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent, Backends, Backends.SourceBackend}

  describe "backend management" do
    setup do
      user = insert(:user)
      [source: insert(:source, user_id: user.id)]
    end
    test "can attach multiple backends to a source", %{source: source} do
      assert {:ok, %SourceBackend{}} = Backends.create_source_backend(source)
      assert {:ok, %SourceBackend{}} = Backends.create_source_backend(source, :webhook)
    end

  end

  describe ("dispatch_ingest") do
    setup do
      source_backend = insert(:source_backend, type: :webhook)
      {:ok, source_backend: source_backend}
    end
    test "dispatch_ingest", %{source_backend: source_backend} do
      Backends.Adaptor.WebhookAdaptor
      |> expect(:ingest, 2, fn _-> :ok end )
      log_event = %LogEvent{}
      assert :ok = Backends.dispatch_ingest(source_backend,[log_event, log_event])

      raise "not impl"
    end
  end


  describe "SourceManager" do
    setup do

    end
    test "can start a SourceSup"
    test "can stop a SourceSup"
    test "can restart a SourceSup"
  end
end
