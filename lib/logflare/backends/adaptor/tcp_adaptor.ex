defmodule Logflare.Backends.Adaptor.TCPAdaptor do
  use TypedStruct

  import Ecto.Changeset

  alias Logflare.Backends.Adaptor.TCPAdaptor.Pool
  alias Logflare.Backends.Adaptor.TCPAdaptor.Syslog

  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:tls, boolean())
    field(:host, String.t())
    field(:port, non_neg_integer())
  end

  @impl true
  def start_link({_source, backend}) do
    Pool.start_link(backend.config)
  end

  @impl true
  def cast_config(params) do
    {%{}, %{tls: :bool, host: :string, port: :integer}}
    |> cast(params, [:tls, :host, :port])
  end

  @impl true
  def validate_config(changeset) do
    changeset
    # Port is at most max(u16)
    |> validate_inclusion(:port, 0..0xFFFF)
  end

  @impl true
  def ingest(pool, log_events, _opts) do
    content = Enum.map(log_events, &Syslog.format(&1, []))

    Pool.send(pool, content)
  end

  @impl true
  def execute_query(_id, _query), do: {:error, :not_implemented}
end
