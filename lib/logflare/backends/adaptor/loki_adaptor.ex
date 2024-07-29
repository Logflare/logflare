defmodule Logflare.Backends.Adaptor.LokiAdaptor do
  @moduledoc """

  Ingestion uses Filebeat HTTP input.

  https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-input-http_endpoint.html

  Basic auth implementation reference:
  https://datatracker.ietf.org/doc/html/rfc7617

  """

  use TypedStruct

  alias Logflare.Backends.Adaptor.WebhookAdaptor

  typedstruct enforce: true do
    field(:url, String.t())
    field(:headers, map(), optional: true)
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
          url: backend.config.url,
          headers: Map.get(backend.config, :headers, %{}),
          format_batch: &format_batch/1,
          gzip: true
        }
    }

    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def format_batch(log_events) do
    # split events
    stream_map =
      for %_{source: %_{name: name, id: source_id}} = event <- log_events, reduce: %{} do
        acc ->
          Map.update(acc, {source_id, name}, [event], fn prev -> [event | prev] end)
      end

    streams =
      for {{_source_id, name}, events} <- stream_map do
        formatted_events =
          Enum.map(events, fn event ->
            ["#{event.body["timestamp"]}", Jason.encode!(event.body)]
          end)

        %{stream: %{source: name}, values: formatted_events}
      end

    %{streams: streams}
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{url: :string, headers: :map}}
    |> Ecto.Changeset.cast(params, [:headers, :url])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
  end
end