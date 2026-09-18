defmodule Logflare.Backends.Adaptor.WebhookV2AdaptorTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.Backends.Backend
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.WebhookV2Adaptor

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "config" do
    test "registers the webhook_v2 backend type" do
      Code.ensure_loaded!(@subject)

      assert Adaptor.get_adaptor(%Backend{type: :webhook_v2}) == @subject
      assert Adaptor.consolidated_ingest?(%Backend{type: :webhook_v2})
    end

    test "applies the webhook defaults and the default batch size" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{url: "https://example.com"})

      assert changeset.valid?

      assert %{http: "http2", gzip: true, format: "json", batch_size: 1_000} =
               Ecto.Changeset.apply_changes(changeset)
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
        batch_size: 500
      }

      sanitized = @subject.sanitize_config_for_display(config)

      assert sanitized.batch_size == 500
      assert sanitized.url == "https://example.com"
      refute sanitized.headers == config.headers
    end
  end

  describe "test_connection/1" do
    test "sends an empty JSON array with a JSON content type" do
      backend =
        insert(:backend, type: :webhook_v2, config: %{url: "https://example.com", format: "json"})

      expect(Client, :send, fn req ->
        assert req[:body] == "[]"
        assert req[:headers]["content-type"] == "application/json"
        {:ok, %Tesla.Env{status: 200}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns an error for a server failure" do
      backend = insert(:backend, type: :webhook_v2, config: %{url: "https://example.com"})

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
          type: :webhook_v2,
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

  defp collect_event_messages(count, acc \\ [])
  defp collect_event_messages(count, acc) when length(acc) >= count, do: acc

  defp collect_event_messages(count, acc) do
    assert_receive {:sent, bodies}, 2_000
    collect_event_messages(count, acc ++ Enum.map(bodies, & &1["event_message"]))
  end
end
