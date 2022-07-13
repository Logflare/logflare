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

  describe "ingestion" do
    setup :set_mimic_global
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      insert(:source_backend, type: :webhook, source_id: source.id, config: %{url: "https://some-url.com"})
      start_supervised!({SourceSup, source})
      {:ok, source: source}
    end

    test "backends receive dispatched log events", %{source: source} do
      Backends.Adaptor.WebhookAdaptor
      |> expect(:ingest, fn _, _ -> :ok end)

      log_event = %LogEvent{}
      assert :ok = Backends.ingest_log_events([log_event, log_event], source)
      :timer.sleep(1500)
    end
  end
end
