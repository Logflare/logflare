defmodule Logflare.Backends.Adaptor.SigNozAdaptor do
  @moduledoc """
  Adaptor sending logs to [SigNoz](https://signoz.io) via OTLP/HTTP
  This adaptor is **ingest-only**

  ## Configuration
  - `:endpoint` - Ingestion URL, obtained via **Settings > Ingestion** in
    [SigNoz Cloud](https://signoz.io/docs/ingestion/signoz-cloud/overview/), or the OTLP/HTTP
    address of a self-managed deployment. The `/v1/logs` path is appended automatically.
  - `:ingestion_key` - Ingestion key used for authentication. Required by SigNoz Cloud, and
    typically unset for self-managed deployments.
  """

  @behaviour Logflare.Backends.Adaptor
  @behaviour Logflare.Backends.Adaptor.HttpBased.Client

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Backend

  @ingestion_key_header "signoz-ingestion-key"
  @logs_path "/v1/logs"

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
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      endpoint: :string,
      ingestion_key: :string
    }

    {existing_config, types}
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:endpoint])
    |> Ecto.Changeset.validate_format(:endpoint, ~r/https?\:\/\/.+/)
    |> validate_endpoint_without_logs_path()
  end

  # Pasting the full logs URL is an easy mistake, and appending to it would silently 404.
  @spec validate_endpoint_without_logs_path(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  defp validate_endpoint_without_logs_path(changeset) do
    endpoint = Ecto.Changeset.get_field(changeset, :endpoint)

    if is_non_empty_binary(endpoint) and
         String.ends_with?(String.trim_trailing(endpoint, "/"), @logs_path) do
      Ecto.Changeset.add_error(
        changeset,
        :endpoint,
        "must not include the #{@logs_path} path, which is appended automatically"
      )
    else
      changeset
    end
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :ingestion_key) do
      Map.put(config, :ingestion_key, "REDACTED")
    else
      config
    end
  end

  @impl Adaptor
  def test_connection(args) do
    OtlpAdaptor.Common.test_connection(__MODULE__, args)
  end

  # `flatten_to_attributes` is forced on rather than exposed as config: SigNoz's ClickHouse logs
  # exporter reads only the record body and attributes, and ignores `event_name`. Under the
  # formatter's default the message goes to `event_name` and the body becomes a kvlist, so the
  # message would be dropped outright and nothing would be searchable.
  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: Path.join(config.endpoint, @logs_path),
      formatter: {ProtobufFormatter, %{flatten_to_attributes: true}},
      headers: headers(config),
      gzip: true,
      json: false
    ]
  end

  @spec headers(map()) :: %{String.t() => String.t()}
  defp headers(%{ingestion_key: key}) when is_non_empty_binary(key),
    do: %{@ingestion_key_header => key}

  defp headers(_config), do: %{}
end
