defmodule Logflare.Backends.Adaptor.DatadogAdaptor do
  @moduledoc """
  Wrapper module for `Logflare.Backends.Adaptor.WebhookAdaptor` to provide API
  for DataDog logs ingestion endpoint.
  """

  use TypedStruct

  alias Logflare.Backends.Adaptor.WebhookAdaptor

  # https://docs.datadoghq.com/api/latest/logs/#send-logs
  @api_url_mapping %{
    "US1" => "https://http-intake.logs.datadoghq.com/api/v2/logs",
    "US3" => "https://http-intake.logs.us3.datadoghq.com/api/v2/logs",
    "US5" => "https://http-intake.logs.us5.datadoghq.com/api/v2/logs",
    "EU" => "https://http-intake.logs.datadoghq.eu/api/v2/logs",
    "AP1" => "https://http-intake.logs.ap1.datadoghq.com/api/v2/logs",
    "US1-FED" => "https://http-intake.logs.ddog-gov.com/api/v2/logs"
  }

  def api_url_mapping, do: @api_url_mapping

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
          url: Map.get(@api_url_mapping, backend.config.region),
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
    {%{}, %{api_key: :string, region: :string}}
    |> Ecto.Changeset.cast(params, [:api_key, :region])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:api_key, :region])
    |> Ecto.Changeset.validate_inclusion(:region, Map.keys(@api_url_mapping))
  end

  defp translate_event(%Logflare.LogEvent{} = le) do
    formatted_ts =
      DateTime.from_unix!(le.body["timestamp"], :microsecond) |> DateTime.to_iso8601()

    %Logflare.LogEvent{
      le
      | body: %{
          message: formatted_ts <> " " <> Jason.encode!(le.body),
          ddsource: "logflare",
          service: le.source.name
        }
    }
  end
end
