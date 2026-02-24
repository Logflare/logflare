defmodule Logflare.Backends.Adaptor.Last9Adaptor do
  @moduledoc """
  Adaptor sending logs to [Last9](https://app.last9.io) via OTLP/HTTP
  This adaptor is **ingest-only**

  ## Configuration
  - `:region` - Region determining endpoint address
  - `:username`, `:password` - Auth data obtained via [Last9 OTel integration panel](https://app.last9.io/integrations?integration=OpenTelemetry)
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Backend

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @region_mapping %{
    "US-WEST-1" => "https://otlp.last9.io",
    "AP-SOUTH-1" => "https://otlp-aps1.last9.io"
  }
  @regions Map.keys(@region_mapping)

  def regions(), do: @regions
  def endpoint_per_region(region), do: @region_mapping[region]

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
      region: :string,
      username: :string,
      password: :string
    }

    {%{}, types}
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:region, :username, :password])
    |> Ecto.Changeset.validate_inclusion(:region, @regions)
  end

  @impl Adaptor
  def redact_config(config) do
    config
    |> Map.put(:password, "REDACTED")
  end

  @impl Adaptor
  def test_connection(args) do
    OtlpAdaptor.Common.test_connection(__MODULE__, args)
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: Path.join(@region_mapping[config.region], "/v1/logs"),
      formatter: ProtobufFormatter,
      basic_auth: [username: config.username, password: config.password],
      gzip: true,
      json: false
    ]
  end
end
