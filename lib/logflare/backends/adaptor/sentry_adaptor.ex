defmodule Logflare.Backends.Adaptor.SentryAdaptor do
  @moduledoc """
  Sentry adaptor for sending logs to Sentry's logging API.

  This adaptor wraps the WebhookAdaptor to provide specific functionality
  for sending logs to Sentry in the expected envelope format.

  ## Configuration

  The adaptor requires a single configuration parameter:

  - `dsn` - The Sentry DSN string in the format:
    `{PROTOCOL}://{PUBLIC_KEY}:{SECRET_KEY}@{HOST}{PATH}/{PROJECT_ID}`

  ## Example DSN

      https://abc123@o123456.ingest.sentry.io/123456
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  @behaviour Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__.Client)
  end

  @impl Adaptor
  def cast_config(params) do
    {%{}, %{dsn: :string}}
    |> Ecto.Changeset.cast(params, [:dsn])
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:dsn])
    |> validate_dsn()
  end

  @impl Adaptor
  def redact_config(config) do
    case Map.get(config, :dsn) || Map.get(config, "dsn") do
      nil ->
        config

      dsn ->
        redacted_dsn = String.replace(dsn, ~r/\/\/([^:]+):[^@]+@/, "//\\1:REDACTED@")
        Map.put(config, :dsn, redacted_dsn)
    end
  end

  defp validate_dsn(%{changes: %{dsn: dsn}} = changeset) do
    case DSN.parse(dsn) do
      {:ok, _parsed_dsn} ->
        changeset

      {:error, reason} ->
        Ecto.Changeset.add_error(changeset, :dsn, "Invalid DSN: #{reason}")
    end
  end

  defp validate_dsn(changeset), do: changeset

  defmodule Client do
    alias Logflare.Backends.Adaptor.HttpBased
    alias Logflare.Backends.Adaptor.SentryAdaptor.DSN
    alias Logflare.Backends.Adaptor.SentryAdaptor.EnvelopeBuilder

    @behaviour HttpBased.Client

    @impl HttpBased.Client
    def send_logs(config, log_events, metadata) do
      config
      |> new()
      |> Tesla.post("", log_events, opts: [metadata: metadata])
    end

    defp new(config) do
      dsn =
        case DSN.parse(config.dsn) do
          {:ok, parsed_dsn} -> parsed_dsn
          {:error, reason} -> raise ArgumentError, "Invalid Sentry DSN: #{reason}"
        end

      HttpBased.Client.new(
        formatter: {EnvelopeBuilder, dsn: dsn.original_dsn},
        url: dsn.endpoint_uri
      )
    end
  end
end
