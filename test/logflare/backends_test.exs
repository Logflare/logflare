defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{Backends, Backends.SourceBackend, Backends.SourceSup}

  @valid_event %{some: "event"}
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

  describe "SourceSup management" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
    end

    test "source_sup_started?/1", %{source: source} do
      assert false == Backends.source_sup_started?(source)
      start_supervised!({SourceSup, source})
      :timer.sleep(400)
      assert true == Backends.source_sup_started?(source)
    end

    test "start_source_sup/1, stop_source_sup/1, restart_source_sup/1", %{source: source} do
      assert :ok = Backends.start_source_sup(source)
      assert {:error, :already_started} = Backends.start_source_sup(source)

      assert :ok = Backends.stop_source_sup(source)
      assert {:error, :not_started} = Backends.stop_source_sup(source)

      assert {:error, :not_started} = Backends.restart_source_sup(source)
      assert :ok = Backends.start_source_sup(source)
      assert :ok = Backends.restart_source_sup(source)
    end
  end

  describe "ingestion" do
    setup :set_mimic_global

    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      start_supervised!({SourceSup, source})
      {:ok, source: source}
    end

    test "gets cached to recent logs", %{source: source} do
      assert :ok = Backends.ingest_logs([%{some: "event"}], source)
      :timer.sleep(1500)
      assert [_] = Backends.list_recent_logs(source)
    end
  end

  describe "ingestion with backend" do
    setup :set_mimic_global

    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)

      insert(:source_backend,
        type: :webhook,
        source_id: source.id,
        config: %{url: "https://some-url.com"}
      )

      start_supervised!({SourceSup, source})
      {:ok, source: source}
    end

    test "backends receive dispatched log events", %{source: source} do
      Backends.Adaptor.WebhookAdaptor
      |> expect(:ingest, fn _pid, [event | _] ->
        if match?(%_{}, event) do
          :ok
        else
          raise "Not a log event struct!"
        end
      end)

      log_event = %{some: "event"}
      assert :ok = Backends.ingest_logs([log_event, log_event], source)
      :timer.sleep(1500)
    end
  end
end
