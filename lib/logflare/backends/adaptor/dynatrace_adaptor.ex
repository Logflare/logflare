defmodule Logflare.Backends.Adaptor.DynatraceAdaptor do
  @moduledoc """
  Wrapper module for `Logflare.Backends.Adaptor.WebhookAdaptor` to provide API
  for the Dynatrace Generic Log Ingest v2 endpoint.

  https://docs.dynatrace.com/docs/shortlink/lma-generic-log-ingest-api

  ## Configuration

  - `:url` - The Dynatrace environment URL, e.g.
    `https://<env-id>.live.dynatrace.com` for SaaS or
    `https://<server>/e/<env-id>` for Managed/ActiveGate. The
    `/api/v2/logs/ingest` path is appended automatically.
  - `:api_token` - API token with the `logs.ingest` (`v2 Logs ingest`) scope.
  """

  @behaviour Logflare.Backends.Adaptor

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  @log_ingest_path "/api/v2/logs/ingest"

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config}) do
    %{
      url: ingest_url(config.url),
      headers: %{"Authorization" => "Api-Token #{config.api_token}"},
      http: "http2",
      gzip: true
    }
  end

  @impl Logflare.Backends.Adaptor
  def pre_ingest(source, _backend, log_events) do
    Enum.map(log_events, &translate_event(source, &1))
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    {existing_config, %{url: :string, api_token: :string}}
    |> Changeset.cast(params, [:url, :api_token])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([:url, :api_token])
    |> Changeset.validate_format(:url, ~r/https\:\/\/.+/,
      message: "must use HTTPS to protect API credentials"
    )
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    Map.put(config, :api_token, "REDACTED")
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.test_connection(backend, [])
  end

  defp ingest_url(url) when is_binary(url) do
    trimmed = String.trim_trailing(url, "/")

    if String.ends_with?(trimmed, @log_ingest_path) do
      trimmed
    else
      trimmed <> @log_ingest_path
    end
  end

  defp translate_event(%Source{} = source, %LogEvent{} = le) do
    %LogEvent{
      le
      | body: %{
          "timestamp" => format_timestamp(le.body["timestamp"]),
          "content" => le.body["event_message"] || le.body["message"] || "",
          "service" => source.service_name || source.name,
          "log.source" => "logflare",
          "data" => le.body
        }
    }
  end

  defp format_timestamp(ts) when is_integer(ts) do
    ts |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()
  end

  defp format_timestamp(ts) when is_binary(ts), do: ts
  defp format_timestamp(_), do: DateTime.utc_now() |> DateTime.to_iso8601()
end
