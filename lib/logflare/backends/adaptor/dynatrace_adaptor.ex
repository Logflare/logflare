defmodule Logflare.Backends.Adaptor.DynatraceAdaptor do
  @moduledoc """
  An **ingest-only** adaptor sending logs to Dynatrace via the
  [Log Monitoring API v2](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/log-monitoring-v2/post-ingest-logs).

  The same API is served by Dynatrace SaaS (`https://<environment-id>.live.dynatrace.com`)
  and by Environment or Cluster ActiveGates (`https://<activegate-host>:9999/e/<environment-id>`),
  so the environment URL is configured directly and the ingest path is appended to it.

  ## Configuration

  - `:url` - Dynatrace environment URL
  - `:api_token` - Access token with the `logs.ingest` scope

  Testing the connection sends an empty batch, which Dynatrace rejects with a `400` JSON
  error envelope only after authorizing the request. That response, or an acceptance, proves
  that the endpoint is reachable and the token is accepted. A `400` without the envelope comes
  from a proxy or web server in front of the API, and is reported as a client error.
  """

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.DynatraceAdaptor.LogRecordFormatter
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Backend
  alias Logflare.Utils.SSRF

  require Logger

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @ingest_path "/api/v2/logs/ingest"

  def child_spec(init_arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [init_arg]}}
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{url: :string, api_token: :string}

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([:url, :api_token])
    |> Changeset.validate_format(:url, ~r/^https:\/\/.+/)
    |> SSRF.validate_url_host(:url)
  end

  @impl Adaptor
  def redact_config(config), do: Map.put(config, :api_token, "REDACTED")

  @impl Adaptor
  def sanitize_config_for_display(config) do
    Adaptor.mask_config_values(config, except: [:url])
  end

  @impl Adaptor
  def test_connection(%Backend{} = backend) do
    case HttpBased.Client.send_events(__MODULE__, [], backend) do
      {:ok, %Tesla.Env{status: status}} when status in 200..299 ->
        :ok

      {:ok, %Tesla.Env{status: 400, body: %{"error" => %{"code" => 400}}}} ->
        :ok

      {:ok, %Tesla.Env{status: status}} = response when status in [401, 403] ->
        log_failure(response, backend)
        {:error, :invalid_token}

      {:ok, %Tesla.Env{status: status}} = response when status in 400..499 ->
        log_failure(response, backend)
        {:error, :http_client_error}

      {:ok, %Tesla.Env{status: status}} = response when status in 500..599 ->
        log_failure(response, backend)
        {:error, :http_server_error}

      response ->
        log_failure(response, backend)
        {:error, :unknown_error}
    end
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: ingest_url(config.url),
      headers: [{"authorization", "Api-Token #{config.api_token}"}],
      formatter: LogRecordFormatter,
      json: true,
      ssrf: true
    ]
  end

  @spec ingest_url(String.t()) :: String.t()
  def ingest_url(url) do
    url
    |> String.trim_trailing("/")
    |> String.replace_suffix(@ingest_path, "")
    |> Kernel.<>(@ingest_path)
  end

  @spec log_failure(Tesla.Env.result(), Backend.t()) :: :ok
  defp log_failure({:ok, %Tesla.Env{status: status, body: resp_body}}, backend) do
    Logger.warning(
      "Unexpected response when testing Dynatrace backend connection: #{status} #{inspect(resp_body)}",
      backend_id: backend.id,
      user_id: backend.user_id
    )
  end

  defp log_failure({:error, reason}, backend) do
    Logger.warning("Request error when testing Dynatrace backend connection: #{inspect(reason)}",
      backend_id: backend.id,
      user_id: backend.user_id
    )
  end
end
