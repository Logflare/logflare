defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent, Backends, Backends.SourceBackend, Backends.SourceSup}

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

  describe "dispatch_ingest" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      insert(:source_backend, type: :webhook, source_id: source.id)
      start_supervised!({SourceSup, source})
      {:ok, source: source}
    end

    test "backends receive ingest log events", %{source: source} do
      Backends.Adaptor.WebhookAdaptor
      |> expect(:ingest, fn _, _ -> :ok end)

      log_event = %LogEvent{}
      assert :ok = Backends.dispatch_ingest([log_event, log_event], source)
    end
  end
end
