defmodule Logflare.Backends.Adaptor.TCPAdaptor do
  @moduledoc """
  TCP backend adaptor that sends out Syslog-formatted messages.
  """

  use Supervisor
  use TypedStruct
  import Ecto.Changeset
  alias Logflare.Backends.Adaptor.TCPAdaptor.{Pool, Pipeline, Syslog}
  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:tls, boolean())
    field(:host, String.t())
    field(:port, non_neg_integer())
    field(:cipher_key, binary())
  end

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    name = Logflare.Backends.via_source(source, __MODULE__, backend)
    Supervisor.start_link(__MODULE__, {source, backend}, name: name)
  end

  @impl Supervisor
  def init({source, backend}) do
    pool_name = Logflare.Backends.via_source(source, Pool, backend)
    pipeline_name = Logflare.Backends.via_source(source, Pipeline, backend)

    children = [
      {Pool, config: backend.config, name: pool_name},
      {Pipeline, source: source, backend: backend, pool: pool_name, name: pipeline_name}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{tls: :bool, host: :string, port: :integer, cipher_key: :string}}
    |> cast(params, [:tls, :host, :port, :cipher_key])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> validate_inclusion(:port, 0..65535)
    |> validate_change(:cipher_key, fn :cipher_key, key ->
      case Base.decode64(key) do
        {:ok, decoded} when byte_size(decoded) == 32 -> []
        :error -> [cipher_key: "must be a base64 encoded 32 byte key"]
      end
    end)
  end

  def ingest(pool, log_events, cipher_key \\ nil) do
    content = log_events |> List.wrap() |> Enum.map(&Syslog.format(&1, cipher_key))
    Pool.send(pool, content)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query, _opts) do
    {:error, :not_implemented}
  end
end
