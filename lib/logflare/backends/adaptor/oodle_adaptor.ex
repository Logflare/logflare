defmodule Logflare.Backends.Adaptor.OodleAdaptor do
  @moduledoc """
  An **ingest-only** adaptor sending logs to Oodle via HTTP.

  ## Configuration
  - `:instance` - Oodle instance ID
  - `:api_key` - Oodle API key used for ingestion
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Backend

  @behaviour Adaptor
  @behaviour HttpBased.Client
  @collector_domain "collector.oodle.ai"

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params) do
    types = %{
      instance: :string,
      api_key: :string
    }

    {%{}, types}
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:instance, :api_key])
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :api_key) do
      Map.put(config, :api_key, "REDACTED")
    else
      config
    end
  end

  @impl Adaptor
  def test_connection(%Backend{} = backend) do
    case HttpBased.Client.send_events(__MODULE__, [], backend) do
      {:ok, %Tesla.Env{status: 200}} ->
        :ok

      {:ok, %Tesla.Env{body: %{"message" => message}}} ->
        {:error, message}

      {:ok, env} ->
        {:error, "Unexpected response: #{env.status} #{inspect(env.body)}"}

      {:error, reason} ->
        {:error, "Request error: #{reason}"}
    end
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    url =
      %URI{
        scheme: "https",
        host: "#{config.instance}-logs.#{@collector_domain}",
        path: "/ingest/v1/logs"
      }
      |> URI.to_string()

    [
      url: url,
      headers: %{
        "X-OODLE-INSTANCE" => config.instance,
        "X-API-KEY" => config.api_key
      },
      gzip: true
    ]
  end
end
