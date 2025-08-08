defmodule Logflare.Cluster.Strategy.GoogleComputeEngine do
  @moduledoc false
  use GenServer
  use Cluster.Strategy

  alias __MODULE__, as: GCE
  alias Cluster.Strategy.State

  @default_polling_interval 120_000
  @default_release_name :node
  defp env_regions, do: Application.get_env(:logflare, __MODULE__)[:regions]
  defp env_zones, do: Application.get_env(:logflare, __MODULE__)[:zones]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init([%State{} = state]) do
    {:ok, load(state)}
  end

  @impl true
  def handle_info(:timeout, state) do
    handle_info(:load, state)
  end

  def handle_info(:load, %State{} = state) do
    {:noreply, load(state)}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def load do
    Process.send(Logflare.ClusterSupervisor, :load, [])
  end

  defp load(%State{} = state) do
    Cluster.Strategy.connect_nodes(
      state.topology,
      state.connect,
      state.list_nodes,
      get_nodes(state)
    )

    Process.send_after(
      self(),
      :load,
      Keyword.get(state.config, :polling_interval, @default_polling_interval)
    )

    state
  end

  def get_nodes(state) do
    metadata = get_metadata()

    if metadata != :error do
      auth_token = Map.get(metadata, "access_token")

      region_nodes =
        Enum.map(env_regions(), fn {region, group_name} ->
          get_region_nodes(state, region, group_name, auth_token)
        end)
        |> Enum.concat()

      zone_nodes =
        Enum.map(env_zones(), fn {zone, group_name} ->
          get_zone_nodes(state, zone, group_name, auth_token)
        end)
        |> Enum.concat()

      region_nodes ++ zone_nodes
    else
      []
    end
  end

  defp get_zone_nodes(state, zone, group_name, auth_token) do
    Cluster.Logger.debug(:gce, "Fetching zone nodes ... ")

    case GCE.ComputeClient.zone_nodes(zone, group_name, auth_token) do
      {:ok, %Tesla.Env{status: 200, body: body}} ->
        get_node_name(state, body, auth_token)

      {:ok, %Tesla.Env{status: status_code, body: body, url: url}} ->
        Cluster.Logger.error(
          :gce,
          "Error getting zone nodes: #{status_code} | #{url} | #{inspect(body)}"
        )

        []

      {:error, message} ->
        Cluster.Logger.error(
          :gce,
          "Error getting zone nodes: #{inspect(message)} | #{zone} | #{group_name}"
        )

        []
    end
  end

  defp get_region_nodes(state, region, group_name, auth_token) do
    Cluster.Logger.debug(:gce, "Fetching region nodes ...")

    case GCE.ComputeClient.region_nodes(region, group_name, auth_token) do
      {:ok, %Tesla.Env{status: 200, body: body}} ->
        get_node_name(state, body, auth_token)

      {:ok, %Tesla.Env{status: status_code, body: body, url: url}} ->
        Cluster.Logger.error(
          :gce,
          "Error getting region nodes: #{status_code} | #{url} | #{inspect(body)}"
        )

        []

      {:error, message} ->
        Cluster.Logger.error(:gce, "Error getting region nodes: #{inspect(message)}")
        []
    end
  end

  defp get_node_name(state, body, auth_token) do
    Cluster.Logger.debug(:gce, "Received body: #{inspect(body)}")

    items = Map.get(body, "items")

    if is_nil(items) do
      []
    else
      Enum.filter(items, fn
        %{"status" => "RUNNING"} -> true
        _ -> false
      end)
      |> Enum.map(fn %{"instance" => url} ->
        case GCE.ComputeClient.node_metadata(url, auth_token) do
          {:ok, %Tesla.Env{status: 200, body: body}} ->
            Cluster.Logger.debug(:gce, "Received instance data: #{inspect(body)}")

            network_ip =
              body
              |> Map.get("networkInterfaces")
              |> hd
              |> Map.get("networkIP")

            Cluster.Logger.debug(:gce, "Node network IP is: #{inspect(network_ip)}")

            release_name = get_release_name(state)
            node_name = :"#{release_name}@#{network_ip}"

            Cluster.Logger.debug(:gce, "Found node: #{inspect(node_name)}")

            node_name

          {:ok, %Tesla.Env{status: status_code, body: body}} ->
            Cluster.Logger.error(
              :gce,
              "Error getting node metadata: #{status_code}: #{inspect(body)}"
            )

            :error

          {:error, response} ->
            Cluster.Logger.error(
              :gce,
              "Error getting node metadata: #{inspect(response)}"
            )

            :error
        end
      end)
    end
  end

  defp get_metadata do
    Cluster.Logger.debug(:gce, "Fetching metadata ...")

    case GCE.AuthClient.metadata() do
      {:ok, %Tesla.Env{status: 200, body: body}} ->
        Cluster.Logger.debug(:gce, "Received access token: #{inspect(body)}")
        body

      {:ok, %Tesla.Env{status: status_code, body: body}} ->
        Cluster.Logger.error(:gce, "Error getting access token: #{status_code}: #{inspect(body)}")
        :error

      {:error, error} ->
        Cluster.Logger.error(:gce, "Error getting access token: #{inspect(error)}")
        :error
    end
  end

  defp get_release_name(%{config: config}) do
    case Keyword.get(config, :release_name) do
      nil ->
        Cluster.Logger.warn(:gce, ":release_name not set in #{__MODULE__} config. Using default.")

        @default_release_name

      name ->
        name
    end
  end
end
