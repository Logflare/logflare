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
    {%{}, %{tls: :bool, host: :string, port: :integer}}
    |> cast(params, [:tls, :host, :port])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    validate_inclusion(changeset, :port, 0..65535)
  end

  def ingest(pool, log_events) do
    content = log_events |> List.wrap() |> Enum.map(&Syslog.format/1)
    Pool.send(pool, content)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query, _opts) do
    {:error, :not_implemented}
  end
end
