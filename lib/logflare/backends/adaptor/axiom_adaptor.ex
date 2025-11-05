defmodule Logflare.Backends.Adaptor.AxiomAdaptor do
  @moduledoc """
  An **ingest-only** adaptor sending logs to Axiom

  This adaptor wraps the WebhookAdaptor to provide specific functionality
  for sending logs to Axiom dataset using their REST API

  ## Configuration

  - `:domain` - The domain specific to the region your account is using,
    Defaults to the US domain (`api.axiom.co`)
  - `:api_token` - Bearer token used for authorization
  - `:dataset_name` - Name of the dataset used for log ingest
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor

  @behaviour Adaptor

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Adaptor
  def format_batch(log_events) do
    for %{body: body} <- log_events do
      Map.update!(body, "timestamp", fn timestamp ->
        timestamp
        |> DateTime.from_unix!(:microsecond)
        |> DateTime.to_iso8601()
      end)
    end
  end

  @impl Adaptor
  def transform_config(%{config: config}) do
    # Endpoint docs: https://axiom.co/docs/restapi/endpoints/ingestIntoDataset
    query = %{
      "timestamp-field" => "timestamp",
      "timestamp-format" => "2006-01-02T15:04:05.999999Z07:00"
    }

    url =
      config
      |> dataset_uri()
      |> URI.append_query(URI.encode_query(query))
      |> URI.to_string()

    %{
      url: url,
      headers: %{
        "content-type" => "application/json",
        "authorization" => "Bearer #{config.api_token}"
      },
      http: "http2",
      gzip: true,
      format_batch: &format_batch/1
    }
  end

  @impl Adaptor
  def cast_config(params) do
    defaults = %{
      domain: "api.axiom.co"
    }

    types = %{
      domain: :string,
      api_token: :string,
      dataset_name: :string
    }

    {%{}, types}
    |> Ecto.Changeset.change(defaults)
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:domain, :api_token, :dataset_name])
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :api_token) do
      Map.put(config, :api_token, "REDACTED")
    else
      config
    end
  end

  @impl Adaptor
  def test_connection({_source, backend}) do
    test_connection(backend)
  end

  def test_connection(%{config: config}) do
    url = config |> dataset_uri() |> URI.to_string()

    result =
      Tesla.client(
        [
          {Tesla.Middleware.BearerAuth, token: config.api_token},
          Tesla.Middleware.JSON,
          {Tesla.Middleware.CompressRequest, format: "gzip"},
          Tesla.Middleware.Telemetry
        ],
        {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}
      )
      |> Tesla.post(url, [])

    case result do
      {:ok, %Tesla.Env{status: 200}} -> :ok
      {:ok, %Tesla.Env{body: %{"message" => message}}} -> {:error, message}
      {:ok, env} -> {:error, "Unexpected response: #{env.status} #{inspect(env.body)}"}
      {:error, reason} -> {:error, "Request error: #{reason}"}
    end
  end

  defp dataset_uri(%{domain: domain, dataset_name: dataset_name}) do
    %URI{
      scheme: "https",
      host: domain,
      path: "/v1/datasets/#{dataset_name}/ingest"
    }
  end
end
