defmodule Logflare.Backends.Adaptor.OtlpAdaptor do
  @moduledoc """
  Adaptor sending logs to ingest compatible with [OTLP](https://opentelemetry.io/docs/specs/otlp/#protocol-details)
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Backend

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @doc """
  Returns a list of supported protocols
  """
  @spec protocols() :: [String.t()]
  def protocols() do
    [
      # "grpc",
      "http/protobuf"
    ]
  end

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
    defaults = %{
      gzip: true,
      protocol: "http/protobuf",
      headers: %{}
    }

    types = %{
      endpoint: :string,
      protocol: :string,
      gzip: :boolean,
      headers: {:map, :string}
    }

    {%{}, types}
    |> Ecto.Changeset.change(defaults)
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:endpoint])
    |> Ecto.Changeset.validate_format(:endpoint, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_inclusion(:protocol, protocols())
  end

  @impl Adaptor
  def redact_config(config) do
    sensitive_headers = ["authorization", "x-api-key", "x-auth-token"]

    Map.update!(config, :headers, fn headers ->
      for {k, v} <- headers, into: %{} do
        if Enum.member?(sensitive_headers, String.downcase(k)) do
          {k, "REDACTED"}
        else
          {k, v}
        end
      end
    end)
  end

  @impl Adaptor
  def test_connection({_source, backend}) do
    test_connection(backend)
  end

  def test_connection(%Backend{} = backend) do
    case HttpBased.Client.send_events(__MODULE__, [], backend) do
      {:ok, %Tesla.Env{status: 200, body: %{partial_success: nil}}} -> :ok
      {:ok, %Tesla.Env{status: 200, body: %{partial_success: %{error_message: ""}}}} -> :ok
      {:ok, env} -> {:error, env}
      {:error, _reason} = err -> err
    end
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: config.endpoint,
      formatter: ProtobufFormatter,
      gzip: config.gzip,
      json: false,
      headers: config.headers
    ]
  end
end
