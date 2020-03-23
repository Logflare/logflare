defmodule Logflare.Cluster.Strategy.GoogleComputeEngine.Staging do
  use GenServer
  use Cluster.Strategy

  alias __MODULE__, as: GCE
  alias Cluster.Strategy.State

  @default_polling_interval 10_000
  @metadata_base_url 'http://metadata.google.internal/computeMetadata/v1'
  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @google_api_base_url 'https://compute.googleapis.com/compute/v1/projects/#{@project_id}'
  @default_release_name :node
  @regions Application.get_env(:logflare, __MODULE__)[:regions]
  @zones Application.get_env(:logflare, __MODULE__)[:zones]

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
    region_nodes =
      Enum.map(@regions, fn {region, group_name} ->
        get_region_nodes(state, region, group_name)
      end)
      |> Enum.concat()

    zone_nodes =
      Enum.map(@zones, fn {zone, group_name} -> get_zone_nodes(state, zone, group_name) end)
      |> Enum.concat()

    region_nodes ++ zone_nodes
  end

  defp get_zone_nodes(state, zone, group_name) do
    Cluster.Logger.debug(:gce, "Loading nodes from GCE API ...")

    auth_token =
      get_metadata()
      |> Map.get("access_token")

    Cluster.Logger.debug(:gce, "Fetching zone nodes ... ")

    case GCE.Client.zone_nodes(zone, group_name, auth_token) do
      {:ok, %Tesla.Env{status: 200, body: body}} ->
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
            # maybe to_charlist(url) here
            case GCE.Client.node_metadata(url, auth_token) do
              {:ok, %Tesla.Env{body: body}} ->
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
                Cluster.Logger.error(:gce, "GCP API error: #{status_code}: #{inspect(body)}")

                :error

              {:error, response} ->
                :error
            end
          end)
        end

      {:ok, %Tesla.Env{status: status_code, body: body}} ->
        Cluster.Logger.error(:gce, "GCP API error: #{status_code}: #{inspect(body)}")
        []

      {:error, message} ->
        Cluster.Logger.error(:gce, "GCP API error: #{inspect(message)}")
        []
    end
  end

  defp get_region_nodes(state, region, group_name) do
    Cluster.Logger.debug(:gce, "Loading nodes from GCE API ...")

    auth_token =
      get_metadata()
      |> Map.get("access_token")

    release_name = get_release_name(state)

    Cluster.Logger.debug(:gce, "Fetching region nodes ...")

    case GCE.Client.region_nodes(region, group_name, auth_token) do
      {:ok, %Tesla.Env{body: body}} ->
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
            case GCE.Client.node_metadata(url, auth_token) do
              {:ok, %Tesla.Env{status: 200, body: body}} ->
                Cluster.Logger.debug(:gce, "Received instance data: #{inspect(body)}")

                network_ip =
                  body
                  |> Map.get("networkInterfaces")
                  |> hd
                  |> Map.get("networkIP")

                Cluster.Logger.debug(:gce, "Node network IP is: #{inspect(network_ip)}")

                node_name = :"#{release_name}@#{network_ip}"

                Cluster.Logger.debug(:gce, "Found node: #{inspect(node_name)}")

                node_name

              {:ok, %Tesla.Env{status: status_code, body: body}} ->
                Cluster.Logger.error(:gce, "GCP API error: #{status_code}: #{inspect(body)}")

                :error

              {:error, response} ->
                :error
            end
          end)
        end

      {:ok, %Tesla.Env{status: status_code, body: body}} ->
        Cluster.Logger.error(:gce, "GCP API error: #{status_code}: #{inspect(body)}")
        []

      {:error, message} ->
        Cluster.Logger.error(:gce, "GCP API error: #{inspect(message)}")
        []
    end
  end

  defp get_metadata() do
    Cluster.Logger.debug(:gce, "Fetching metadata ...")

    case GCE.Client.metadata() do
      {:ok, response} ->
        Cluster.Logger.debug(:gce, "Received body: #{response.body}")
        response.body

      {:error, error} ->
        Cluster.Logger.debug(:gce, "Error getting metadata: #{inspect(error)}")
        %{}
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
