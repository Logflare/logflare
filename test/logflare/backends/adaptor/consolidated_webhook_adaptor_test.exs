defmodule Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptorTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.Backends.Backend
  alias Logflare.Backends.CircuitBreaker
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor
  @drop_sampled_event [:logflare, :logs, :ingest_logs, :drop_sampled]

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "config" do
    test "registers the consolidated_webhook backend type" do
      Code.ensure_loaded!(@subject)

      assert Adaptor.get_adaptor(%Backend{type: :consolidated_webhook}) == @subject
      assert Adaptor.consolidated_ingest?(%Backend{type: :consolidated_webhook})
    end

    test "applies the webhook defaults and the default batch size" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{url: "https://example.com"})

      assert changeset.valid?

      assert %{
               http: "http2",
               gzip: true,
               format: "json",
               batch_size: 1_000,
               sample_percentage: 100.0
             } = Ecto.Changeset.apply_changes(changeset)
    end

    test "accepts a custom sample percentage" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "url" => "https://example.com",
          "sample_percentage" => "12.5"
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :sample_percentage) == 12.5
    end

    test "rejects a sample percentage outside the allowed range" do
      for sample_percentage <- [0, -5, 100.1] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{
            url: "https://example.com",
            sample_percentage: sample_percentage
          })

        refute changeset.valid?
        assert %{sample_percentage: [_]} = errors_on(changeset)
      end
    end

    test "accepts a custom batch size" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "url" => "https://example.com",
          "batch_size" => "5000"
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :batch_size) == 5_000
    end

    test "rejects a batch size outside the allowed range" do
      for batch_size <- [0, -1, @subject.max_batch_size() + 1] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{
            url: "https://example.com",
            batch_size: batch_size
          })

        refute changeset.valid?
        assert %{batch_size: [_]} = errors_on(changeset)
      end
    end

    test "requires a valid url" do
      refute Adaptor.cast_and_validate_config(@subject, %{}).valid?
      refute Adaptor.cast_and_validate_config(@subject, %{url: "not-a-url"}).valid?
    end

    test "redacts secret headers and keeps the batch size" do
      config = %{
        url: "https://user:pass@example.com",
        headers: %{"authorization" => "Bearer secret", "x-custom" => "visible"},
        batch_size: 500
      }

      assert %{
               url: "https://REDACTED@example.com",
               headers: %{"authorization" => "REDACTED", "x-custom" => "visible"},
               batch_size: 500
             } = @subject.redact_config(config)
    end

    test "shows the batch size and masks the headers for display" do
      config = %{
        url: "https://example.com",
        headers: %{"authorization" => "Bearer secret"},
        batch_size: 500,
        sample_percentage: 25.0
      }

      sanitized = @subject.sanitize_config_for_display(config)

      assert sanitized.batch_size == 500
      assert sanitized.sample_percentage == 25.0
      assert sanitized.url == "https://example.com"
      refute sanitized.headers == config.headers
    end
  end

  describe "pre_ingest/3" do
    setup do
      source = insert(:source, user: insert(:user))
      events = for _n <- 1..5, do: build(:log_event, source: source)
      ref = :telemetry_test.attach_event_handlers(self(), [@drop_sampled_event])
      on_exit(fn -> :telemetry.detach(ref) end)

      [source: source, events: events, ref: ref]
    end

    test "keeps all events at 100 percent", %{source: source, events: events, ref: ref} do
      backend = %Backend{id: 1, type: :consolidated_webhook, config: %{sample_percentage: 100.0}}

      assert @subject.pre_ingest(source, backend, events) == events
      refute_receive {@drop_sampled_event, ^ref, _, _}
    end

    test "keeps all events when the config has no sample percentage", %{
      source: source,
      events: events
    } do
      backend = %Backend{
        id: 1,
        type: :consolidated_webhook,
        config: %{url: "https://example.com"}
      }

      assert @subject.pre_ingest(source, backend, events) == events
    end

    test "keeps about the configured percentage and reports the dropped count", %{
      source: source,
      ref: ref
    } do
      backend = %Backend{id: 1, type: :consolidated_webhook, config: %{sample_percentage: 50.0}}
      events = for _n <- 1..10_000, do: build(:log_event, source: source)

      kept = @subject.pre_ingest(source, backend, events)

      assert length(kept) in 4_500..5_500
      assert kept -- events == []
      assert kept == Enum.filter(events, &(&1 in kept))

      dropped = length(events) - length(kept)

      assert_receive {@drop_sampled_event, ^ref, %{count: ^dropped},
                      %{backend_id: 1, backend_type: :consolidated_webhook}}
    end

    test "returns an empty list for an empty batch", %{source: source} do
      backend = %Backend{id: 1, type: :consolidated_webhook, config: %{sample_percentage: 50.0}}

      assert @subject.pre_ingest(source, backend, []) == []
    end
  end

  describe "supervision" do
    test "starts a circuit breaker and opens it after repeated transient failures" do
      backend =
        insert(:backend, type: :consolidated_webhook, config: %{url: "https://example.com"})

      start_supervised!({@subject, backend})

      assert %CircuitBreaker{backend_type: :consolidated_webhook} =
               CircuitBreaker.get_state(backend)

      assert :ok = CircuitBreaker.check(backend)

      assert :ok = CircuitBreaker.trip(backend)
      assert {:error, :circuit_open, _blocked_until} = CircuitBreaker.check(backend)
    end
  end

  describe "test_connection/1" do
    test "sends an empty JSON array with a JSON content type" do
      backend =
        insert(:backend,
          type: :consolidated_webhook,
          config: %{url: "https://example.com", format: "json"}
        )

      expect(Client, :send, fn req ->
        assert req[:body] == "[]"
        assert req[:headers]["content-type"] == "application/json"
        {:ok, %Tesla.Env{status: 200}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns an error for a server failure" do
      backend =
        insert(:backend, type: :consolidated_webhook, config: %{url: "https://example.com"})

      expect(Client, :send, fn _req -> {:ok, %Tesla.Env{status: 503, body: "down"}} end)

      assert {:error, :http_server_error} = @subject.test_connection(backend)
    end
  end

  describe "ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      other_source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :consolidated_webhook,
          user: user,
          sources: [source, other_source],
          config: %{http: "http1", url: "https://example.com", format: "json", gzip: true}
        )

      start_supervised!({@subject, backend})

      [source: source, other_source: other_source, backend: backend]
    end

    test "sends events from the consolidated queue as a JSON array", %{source: source} do
      this = self()

      stub(Client, :send, fn req ->
        send(this, {:sent, req})
        {:ok, %Tesla.Env{status: 200}}
      end)

      event = build(:log_event, source: source, message: "v2 event")

      assert {:ok, _} = Backends.ingest_logs([event], source)
      assert_receive {:sent, req}, 2_000

      assert req[:url] == "https://example.com"
      assert req[:headers]["content-type"] == "application/json"
      assert [%{"event_message" => "v2 event"}] = Jason.decode!(req[:body])
    end

    test "a backend created with a batch size uses it at once" do
      this = self()
      user = insert(:user)
      source = insert(:source, user: user)

      stub(Client, :send, fn req ->
        send(this, {:sent, Jason.decode!(req[:body])})
        {:ok, %Tesla.Env{status: 200}}
      end)

      {:ok, backend} =
        Backends.create_backend(user, %{
          name: "small batches",
          type: :consolidated_webhook,
          config: %{url: "https://example.com", batch_size: 2}
        })

      on_exit(fn -> ConsolidatedSup.stop_pipeline(backend.id) end)
      {:ok, _source} = Backends.update_source_backends(source, [backend])

      events = for n <- 1..6, do: build(:log_event, source: source, message: "event #{n}")
      assert {:ok, 6} = Backends.ingest_logs(events, source)

      requests = collect_requests(6)

      assert Enum.all?(requests, &(length(&1) <= 2))
      assert requests |> List.flatten() |> length() == 6
    end

    test "drops sampled-out events before the queue", %{source: source, backend: backend} do
      reject(&Client.send/1)

      {:ok, _backend} =
        Backends.update_backend(backend, %{config: %{sample_percentage: 0.000001}})

      ref = :telemetry_test.attach_event_handlers(self(), [@drop_sampled_event])
      on_exit(fn -> :telemetry.detach(ref) end)

      events = for _n <- 1..50, do: build(:log_event, source: source)
      backend_id = backend.id

      assert {:ok, 50} = Backends.ingest_logs(events, source)
      assert_receive {@drop_sampled_event, ^ref, %{count: 50}, %{backend_id: ^backend_id}}
      assert IngestEventQueue.total_pending({:consolidated, backend.id}) == 0
    end

    test "batches events from more than one source into the same pipeline", %{
      source: source,
      other_source: other_source
    } do
      this = self()

      stub(Client, :send, fn req ->
        send(this, {:sent, Jason.decode!(req[:body])})
        {:ok, %Tesla.Env{status: 200}}
      end)

      first = build(:log_event, source: source, message: "first source")
      second = build(:log_event, source: other_source, message: "second source")

      assert {:ok, _} = Backends.ingest_logs([first], source)
      assert {:ok, _} = Backends.ingest_logs([second], other_source)

      messages = collect_event_messages(2)

      assert Enum.sort(messages) == ["first source", "second source"]
    end
  end

  defp collect_requests(count, acc \\ []) do
    if acc |> List.flatten() |> length() >= count do
      acc
    else
      assert_receive {:sent, bodies}, 2_000
      collect_requests(count, [bodies | acc])
    end
  end

  defp collect_event_messages(count, acc \\ [])
  defp collect_event_messages(count, acc) when length(acc) >= count, do: acc

  defp collect_event_messages(count, acc) do
    assert_receive {:sent, bodies}, 2_000
    collect_event_messages(count, acc ++ Enum.map(bodies, & &1["event_message"]))
  end
end
