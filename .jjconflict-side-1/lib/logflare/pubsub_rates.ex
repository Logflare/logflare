defmodule Logflare.PubSubRates do
  @moduledoc false
  use Supervisor

  alias Logflare.PubSubRates

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      PubSubRates.Cache
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
