defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor do
  @moduledoc false
  # Supervision tree for `Logflare.Backends.Adaptor.PostgresAdaptor` processes
  #
  # Do not use outside of the `PostgresAdaptor` internal modules
  use Supervisor

  alias Logflare.Backends.Backend

  @repo_sup __MODULE__.Repos
  @registry Logflare.Backends.SourceRegistry

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args)
  end

  def init(_args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @repo_sup}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Start repository as a supervised process
  """
  @spec start_child({module(), term()}) :: DynamicSupervisor.on_start_child()
  def start_child(spec),
    do: DynamicSupervisor.start_child(@repo_sup, spec)

  @doc """
  Get PID and metadata for repository process for given backend
  """
  @spec get(Backend.t()) :: {:ok, pid(), term()}
  def get(%Backend{id: id}) do
    case Registry.lookup(@registry, {__MODULE__, id}) do
      [{pid, meta}] -> {:ok, pid, meta}
      [] -> :error
    end
  end

  @doc """
  Create `:via`-tuple for given backend that will be used to register repository process
  """
  @spec via(Backend.t()) :: {:via, module(), term()}
  def via(%Backend{id: id}), do: {:via, Registry, {@registry, {__MODULE__, id}}}
end
