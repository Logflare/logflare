defmodule Logflare.Backends.Adaptor.IncidentioAdaptor do
  @moduledoc """
  Adaptor for Incident.io Alert Events
  """

  use TypedStruct
  use Phoenix.VerifiedRoutes,
    router: LogflareWeb.Router,
    endpoint: LogflareWeb.Endpoint


  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Utils

  typedstruct enforce: true do
    field(:api_token, String.t())
    field(:alert_source_config_id, String.t())
    field(:metadata, map(), optional: true)
    field(:title, String.t(), optional: true)
    field(:description, String.t(), optional: true)
    field(:source_url, String.t(), optional: true)
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
    backend = %{backend | config: transform_config(backend)}

    WebhookAdaptor.start_link({source, backend})
  end


  @impl Logflare.Backends.Adaptor
  def send_alert(backend, alert_query, results) do
    config_map = transform_config(backend)
    |> Map.put(:source_url, url(~p"/alerts/#{alert_query.id}"))
    |> Map.put(:title, alert_query.name)
    |> Map.put(:description, alert_query.description)

    config = for {key, value} <- config_map, do: {key, value}
    config = Keyword.put(config, :body, format_batch(results, config_map))
    WebhookAdaptor.Client.send(config)
  end

  @impl Logflare.Backends.Adaptor
  def format_batch(log_events_or_rows, config) do
    batch = Enum.map(log_events_or_rows, fn
      %_{body: body} -> body
    other -> other
  end)
    # Convert log events to incident.io alert event format
    hash = :erlang.phash2(batch)
    %{
      "deduplication_key" =>  "#{hash}",
      "description" => Map.get(config, :description),
      "metadata" =>  %{
        "data" => batch
      },
      "source_url" => Map.get(config, :source_url),
      "status" => "firing",
      "title" => Map.get(config, :title, "#{Enum.count(batch)} events detected")
    }
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config} = backend) do
    url = "https://api.incident.io/v2/alert_events/http/#{config["alert_source_config_id"]}"
    headers = %{
      "Authorization" => "Bearer #{config.api_token}"
    }
    enriched_config = Map.put(config, :source_url, url(~p"/backends/#{backend.id}"))
    %{
      url: url,
      headers: headers,
      format_batch: &(format_batch(&1, enriched_config)),
      gzip: true,
      http: "http1"
    }
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{api_token: :string, alert_source_config_id: :string, metadata: :map}}
    |> Ecto.Changeset.cast(params, [:api_token, :alert_source_config_id, :metadata])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:api_token, :alert_source_config_id])
  end
end
