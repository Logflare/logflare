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

    test "delete backend", %{source: source} do
      assert {:ok, sb} =
               Backends.create_source_backend(source, :webhook, %{url: "http://some.url"})

      assert {:ok, %SourceBackend{}} = Backends.delete_source_backend(sb)
      assert Backends.get_source_backend(sb.id) == nil
    end

    test "can attach multiple backends to a source", %{source: source} do
      assert {:ok, %SourceBackend{}} =
               Backends.create_source_backend(source, :webhook, %{url: "http://some.url"})

      assert {:ok, %SourceBackend{}} =
               Backends.create_source_backend(source, :webhook, %{url: "http://some.url"})

      assert [%{config: %{url: "http" <> _}}, _] = Backends.list_source_backends(source)
    end

    test "validates config correctly for websocket backends", %{source: source} do
      assert {:ok, source_backend} =
               Backends.create_source_backend(source, :webhook, %{url: "http://example.com"})

      assert %SourceBackend{config: %{url: "http://example.com"}} = source_backend

      assert {:error, %Ecto.Changeset{}} =
               Backends.create_source_backend(source, :webhook, %{url: nil})

      assert {:ok,
              %SourceBackend{
                config: %{
                  url: "http://changed.com"
                }
              }} =
               Backends.update_source_backend_config(source_backend, %{url: "http://changed.com"})

      assert {:error, %Ecto.Changeset{}} =
               Backends.update_source_backend_config(source_backend, %{url: nil})

      # unchanged
      assert %SourceBackend{config: %{url: "http" <> _}} =
               Backends.get_source_backend(source_backend.id)
    end

    test "validates config correctly for postgres backends", %{source: source} do
      assert {:ok, source_backend} =
               Backends.create_source_backend(source, :postgres, %{url: "postgresql://host"})

      assert %SourceBackend{config: %{url: "postgresql://host"}, type: :postgres} = source_backend

      assert {:error, %Ecto.Changeset{}} =
               Backends.create_source_backend(source, :postgres, %{url: nil})

      assert {:ok, %SourceBackend{config: %{url: "postgresql://changed"}, type: :postgres}} =
               Backends.update_source_backend_config(source_backend, %{
                 url: "postgresql://changed"
               })

      assert {:error, %Ecto.Changeset{}} =
               Backends.update_source_backend_config(source_backend, %{url: nil})

      # unchanged
      assert %SourceBackend{config: %{url: "postgresql" <> _}} =
               Backends.get_source_backend(source_backend.id)
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
      assert {:ok, _} = Backends.start_source_sup(source)
      assert {:error, :already_started} = Backends.start_source_sup(source)

      assert :ok = Backends.stop_source_sup(source)
      assert {:error, :not_started} = Backends.stop_source_sup(source)

      assert {:error, :not_started} = Backends.restart_source_sup(source)
      assert {:ok, _} = Backends.start_source_sup(source)
      assert :ok = Backends.restart_source_sup(source)
    end
  end

  describe "ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      start_supervised!({SourceSup, source})
      {:ok, source: source}
    end

    test "correctly retains the 100 items", %{source: source} do
      events = for n <- 1..105, do: %{n: n}
      assert :ok = Backends.ingest_logs(events, source)
      :timer.sleep(1500)
      cached = Backends.list_recent_logs(source)
      assert length(cached) == 100
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

      assert :ok = Backends.ingest_logs([@valid_event, @valid_event], source)
      :timer.sleep(1500)
    end
  end
end
