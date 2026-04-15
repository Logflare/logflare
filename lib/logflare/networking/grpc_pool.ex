defmodule Logflare.Networking.GrpcPool do
  @moduledoc false

  use Supervisor

  alias Logflare.Networking.GrpcChannelMonitor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    url = Keyword.fetch!(opts, :url)
    size = Keyword.get(opts, :size, System.schedulers_online())
    registry = registry_name(name)

    ref = :atomics.new(1, signed: false)
    # To start from 0 on next add_get
    :ok = :atomics.sub(ref, 1, 1)
    :persistent_term.put({name, :counter}, ref)
    :persistent_term.put({name, :size}, size)

    monitors =
      for idx <- 0..(size - 1) do
        %{
          id: {GrpcChannelMonitor, idx},
          start: {GrpcChannelMonitor, :start_link, [{idx, url, registry}]},
          restart: :permanent
        }
      end

    Supervisor.init([{Registry, keys: :unique, name: registry} | monitors],
      strategy: :one_for_one
    )
  end

  @spec get_channel(module()) :: {:ok, GRPC.Channel.t()} | {:error, :not_connected}
  def get_channel(name) do
    ref = :persistent_term.get({name, :counter})
    size = :persistent_term.get({name, :size})
    idx = rem(:atomics.add_get(ref, 1, 1), size)

    case Registry.lookup(registry_name(name), idx) do
      [{_pid, channel}] -> {:ok, channel}
      [] -> {:error, :not_connected}
    end
  end

  defp registry_name(name), do: Module.concat(name, Registry)
end
