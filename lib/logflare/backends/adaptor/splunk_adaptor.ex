defmodule Logflare.Backends.Adaptor.SplunkAdaptor do
  @moduledoc """
  An **ingest-only** adaptor sending logs to Splunk via the
  [HTTP Event Collector](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector) (HEC) API.

  The same API is served by Splunk Enterprise (`https://<host>:8088/services/collector/event`)
  and Splunk Cloud Platform (`https://http-inputs-<stack>.splunkcloud.com/services/collector/event`),
  so the full endpoint URL is configured directly.

  ## Configuration

  - `:url` - Full HEC endpoint URL
  - `:token` - HEC token
  - `:index` - Optional target index, must be allowed by the HEC token
  - `:source` - Optional event source, defaults to the Logflare source name
  - `:sourcetype` - Optional event sourcetype, defaults to `_json`
  - `:host` - Optional event host, defaults to the HEC input setting
  """

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.SplunkAdaptor.HecFormatter
  alias Logflare.Backends.Backend
  alias Logflare.Utils.SSRF

  require Logger

  @behaviour Adaptor
  @behaviour HttpBased.Client

  # https://docs.splunk.com/Documentation/Splunk/latest/Data/TroubleshootHTTPEventCollector
  @no_data_code 5
  @invalid_token_codes [1, 2, 3, 4]

  def child_spec(init_arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [init_arg]}}
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      url: :string,
      token: :string,
      index: :string,
      source: :string,
      sourcetype: :string,
      host: :string
    }

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([:url, :token])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_no_ssrf()
  end

  @impl Adaptor
  def redact_config(config), do: Map.put(config, :token, "REDACTED")

  @impl Adaptor
  def test_connection(%Backend{} = backend) do
    # An empty batch either succeeds or is rejected with a `no data` error. Both prove
    # that the endpoint is reachable and the token is accepted.
    case HttpBased.Client.send_events(__MODULE__, [], backend) do
      {:ok, %Tesla.Env{status: 200}} ->
        :ok

      {:ok, %Tesla.Env{status: 400, body: %{"code" => @no_data_code}}} ->
        :ok

      {:ok, %Tesla.Env{status: status, body: %{"code" => code}}} = response
      when status in 400..499 and code in @invalid_token_codes ->
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
      url: config.url,
      headers: [{"authorization", "Splunk #{config.token}"}],
      formatter: {HecFormatter, config: config},
      json: true,
      gzip: true,
      ssrf: true
    ]
  end

  @spec validate_no_ssrf(Changeset.t()) :: Changeset.t()
  defp validate_no_ssrf(changeset) do
    url = Changeset.get_field(changeset, :url)

    # a missing or malformed url is already reported by the preceding validations
    if is_nil(url) or Keyword.has_key?(changeset.errors, :url) do
      changeset
    else
      case SSRF.safe_resolve(URI.parse(url).host) do
        {:ok, _addr} -> changeset
        {:error, reason} -> Changeset.add_error(changeset, :url, reason, validation: :ssrf)
      end
    end
  end

  @spec log_failure(Tesla.Env.result(), Backend.t()) :: :ok
  defp log_failure({:ok, %Tesla.Env{status: status, body: resp_body}}, backend) do
    Logger.warning(
      "Unexpected response when testing Splunk backend connection: #{status} #{inspect(resp_body)}",
      backend_id: backend.id,
      user_id: backend.user_id
    )
  end

  defp log_failure({:error, reason}, backend) do
    Logger.warning("Request error when testing Splunk backend connection: #{inspect(reason)}",
      backend_id: backend.id,
      user_id: backend.user_id
    )
  end
end
