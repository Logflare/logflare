defmodule Logflare.Backends.Spool.ProducerSup do
  @moduledoc false

  use Supervisor

  alias Logflare.Backends.Spool.PartitionSupervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    Supervisor.init([PartitionSupervisor], strategy: :one_for_one)
  end
end
