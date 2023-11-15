defmodule Logflare.Cluster.PostgresStrategy do
  @moduledoc """
  A libcluster strategy that uses Postgres LISTEN/NOTIFY to determine the cluster topology.

  This strategy operates by having all nodes in the cluster listen for and send notifications to a shared Postgres channel.

  When a node comes online, it begins to broadcast its name in a "heartbeat" message to the channel. All other nodes that receive this message attempt to connect to it.

  This strategy does not check connectivity between nodes and does not disconnect them

  ## Options

  * `heartbeat_interval` - The interval at which to send heartbeat messages in milliseconds (optional; default: 5_000)

  The magic cookie is used as the channel name.
  References: https://github.com/supabase/supavisor/blob/main/lib/cluster/strategy/postgres.ex
  """
  use GenServer

  alias Cluster.Strategy
  alias Cluster.Logger
  alias Postgrex, as: P

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  def init([state]) do
    url = get_db_url()

    opts =
      Ecto.Repo.Supervisor.parse_url(url)
      |> Keyword.put_new(:parameters, application_name: "cluster_node_#{node()}")
      |> Keyword.put_new(:auto_reconnect, true)

    new_config =
      state.config
      |> Keyword.put_new(:heartbeat_interval, 5_000)
      |> Keyword.put(:channel_name, Atom.to_string(Node.get_cookie()))
      |> Keyword.put(:url, url)

    meta = %{
      opts: fn -> opts end,
      conn: nil,
      conn_notif: nil,
      heartbeat_ref: make_ref()
    }

    {:ok, %{state | config: new_config, meta: meta}, {:continue, :connect}}
  end

  def handle_continue(:connect, state) do
    with {:ok, conn} <- P.start_link(state.meta.opts.()),
         {:ok, conn_notif} <- P.Notifications.start_link(state.meta.opts.()),
         {_, _} <- P.Notifications.listen(conn_notif, state.config[:channel_name]) do
      Logger.info(state.topology, "Connected to Postgres database")

      meta = %{
        state.meta
        | conn: conn,
          conn_notif: conn_notif,
          heartbeat_ref: heartbeat(0)
      }

      {:noreply, put_in(state.meta, meta)}
    else
      reason ->
        Logger.error(state.topology, "Failed to connect to Postgres: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_info(:heartbeat, state) do
    Process.cancel_timer(state.meta.heartbeat_ref)
    P.query(state.meta.conn, "NOTIFY #{state.config[:channel_name]}, '#{node()}'", [])
    ref = heartbeat(state.config[:heartbeat_interval])
    {:noreply, put_in(state.meta.heartbeat_ref, ref)}
  end

  def handle_info({:notification, _pid, _ref, _channel, node_str}, state) do
    node = String.to_atom(node_str)

    if node != node() do
      topology = state.topology
      Logger.debug(topology, "Trying to connect to node: #{node}")

      case Strategy.connect_nodes(topology, state.connect, state.list_nodes, [node]) do
        :ok ->
          Logger.debug(topology, "Connected to node: #{node}")

        {:error, _} ->
          Logger.error(topology, "Failed to connect to node: #{node}")
      end
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.error(state.topology, "Received unknown message #{inspect(msg, pretty: true)}")
    {:noreply, state}
  end

  ### Internal functions
  @spec heartbeat(non_neg_integer()) :: reference()
  defp heartbeat(interval) when interval >= 0 do
    Process.send_after(self(), :heartbeat, interval)
  end

  @spec get_db_url() :: String.t()
  def get_db_url do
    username = Application.get_env(:logflare, Logflare.Repo)[:username]
    password = Application.get_env(:logflare, Logflare.Repo)[:password]
    port = Application.get_env(:logflare, Logflare.Repo)[:port]
    hostname = Application.get_env(:logflare, Logflare.Repo)[:hostname]
    database = Application.get_env(:logflare, Logflare.Repo)[:database]
    ~s|postgresql://#{username}:#{password}@#{hostname}:#{port}/#{database}|
  end
end
