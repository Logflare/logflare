defmodule Logflare.Backends.Adaptor.LokiAdaptor do
  @moduledoc """

  Ingestion uses Filebeat HTTP input.

  https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-input-http_endpoint.html

  Basic auth implementation reference:
  https://datatracker.ietf.org/doc/html/rfc7617

  """

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Utils
  alias Logflare.Sources

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
  def format_batch(log_events) do
    # split events
    stream_map =
      for %_{source_id: source_id} = event <- log_events,
          %_{name: name, service_name: service_name} = Sources.Cache.get_by_id(source_id),
          reduce: %{} do
        acc ->
          Map.update(acc, {source_id, service_name || name}, [event], fn prev ->
            [event | prev]
          end)
      end

    streams =
      for {{_source_id, name}, events} <- stream_map do
        formatted_events =
          Enum.map(events, fn event ->
            formatted_ts = event.body["timestamp"] * 1000

            structured_metadata =
              event.body
              |> Iteraptor.to_flatmap(delimiter: "_")
              |> Iteraptor.map(fn
                {_, v} when is_binary(v) -> v
                {_, v} -> Utils.stringify(v)
              end)
              |> Map.drop(["timestamp", "event_message"])

            ["#{formatted_ts}", event.body["event_message"] || "", structured_metadata]
          end)

        %{
          stream: %{source: "supabase", service: name},
          values: formatted_events
        }
      end

    %{streams: streams}
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config}) do
    basic_auth = Utils.encode_basic_auth(config)

    headers = Map.get(config, :headers, %{})

    headers =
      if basic_auth do
        Map.put(headers, "Authorization", "Basic #{basic_auth}")
      else
        headers
      end

    %{
      url: config.url,
      headers: headers,
      format_batch: &format_batch/1,
      gzip: true,
      http: "http1"
    }
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{url: :string, headers: :map, username: :string, password: :string}}
    |> Ecto.Changeset.cast(params, [:headers, :url, :username, :password])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
  end

  defp validate_user_pass(changeset) do
    user = Ecto.Changeset.get_field(changeset, :username)
    pass = Ecto.Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for basic auth"

      changeset
      |> Ecto.Changeset.add_error(:username, msg)
      |> Ecto.Changeset.add_error(:password, msg)
    else
      changeset
    end
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    if Map.get(config, :password) do
      Map.put(config, :password, "REDACTED")
    else
      config
    end
  end
end
