defmodule Logflare.Backends.Adaptor.OtlpAdaptor do
  @moduledoc """
  Adaptor sending logs to ingest compatible with [OTLP](https://opentelemetry.io/docs/specs/otlp/#protocol-details)
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Backend
  alias Logflare.Utils

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @sensitive_headers ["authorization", "x-api-key", "x-auth-token"]

  @doc """
  Returns a list of supported protocols
  """
  @spec protocols() :: [String.t()]
  def protocols do
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
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      endpoint: :string,
      protocol: :string,
      gzip: :boolean,
      headers: {:map, :string}
    }

    {existing_config, types}
    |> Ecto.Changeset.cast(params, Map.keys(types))
    |> normalize_header_keys()
    |> Utils.default_field_value(:gzip, true)
    |> Utils.default_field_value(:protocol, "http/protobuf")
    |> Utils.default_field_value(:headers, %{})
  end

  # Canonicalizes submitted header names to lower case so stored config cannot
  # hold case-variant duplicates of the same header (e.g. "Content-Type" and
  # "content-type"), matching the form used on the wire.
  defp normalize_header_keys(changeset) do
    case Ecto.Changeset.get_change(changeset, :headers) do
      nil -> changeset
      headers -> Ecto.Changeset.put_change(changeset, :headers, Headers.normalize_keys(headers))
    end
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
    Map.update!(config, :headers, &redact_headers/1)
  end

  defp redact_headers(headers) do
    for {k, v} <- headers, into: %{}, do: redact_header(k, v)
  end

  defp redact_header(k, v) do
    if Enum.member?(@sensitive_headers, String.downcase(k)) do
      {k, "REDACTED"}
    else
      {k, v}
    end
  end

  @impl Adaptor
  def test_connection(args) do
    __MODULE__.Common.test_connection(__MODULE__, args)
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
