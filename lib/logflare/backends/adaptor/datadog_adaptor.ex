defmodule Logflare.Backends.Adaptor.DatadogAdaptor do
  @moduledoc """
  Wrapper module for `Logflare.Backends.Adaptor.WebhookAdaptor` to provide API
  for DataDog logs ingestion endpoint.
  """

  use TypedStruct

  alias Logflare.Backends.Adaptor.WebhookAdaptor

  # https://docs.datadoghq.com/api/latest/logs/#send-logs
  @api_url "https://http-intake.logs.us5.datadoghq.com/api/v2/logs"

  typedstruct enforce: true do
    field(:api_key, String.t())
  end

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{
      backend
      | config: %{
          url: @api_url,
          headers: %{"dd-api-key" => backend.config.api_key}
        }
    }

    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, log_events, opts) do
    new_events =
      Enum.map(log_events, &translate_event/1)

    WebhookAdaptor.ingest(pid, new_events, opts)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{api_key: :string}}
    |> Ecto.Changeset.cast(params, [:api_key])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:api_key])
  end

  defp translate_event(%Logflare.LogEvent{} = le) do
    formatted_ts =
      DateTime.from_unix!(le.body["timestamp"], :microsecond) |> DateTime.to_iso8601()

    %Logflare.LogEvent{
      le
      | body: %{
          message: formatted_ts <> " " <> Jason.encode!(le.body),
          ddsource: "logflare",
          hostname: "logflare",
          service: le.source.name
        }
    }
  end
end
