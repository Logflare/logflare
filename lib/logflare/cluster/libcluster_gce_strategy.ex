defmodule Logflare.Cluster.Strategy.GoogleComputeEngine do
  use GenServer
  use Cluster.Strategy

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
    Cluster.Logger.debug(:gce, "Loading nodes from GCE API...")

    auth_token =
      get_metadata('/instance/service-accounts/default/token')
      |> Jason.decode!()
      |> Map.get("access_token")

    release_name = get_release_name(state)

    headers = [{'Authorization', 'Bearer #{auth_token}'}]

    url = @google_api_base_url ++ '/zones/#{zone}/instanceGroups/#{group_name}/listInstances'

    Cluster.Logger.debug(:gce, "Fetching instances from #{inspect(url)}")

    case :httpc.request(:post, {url, headers, 'application/json', ''}, [], []) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        Cluster.Logger.debug(:gce, "    Received body: #{inspect(body)}")

        items =
          Jason.decode!(body)
          |> Map.get("items")

        if is_nil(items) do
          []
        else
          Enum.filter(items, fn
            %{"status" => "RUNNING"} -> true
            _ -> false
          end)
          |> Enum.map(fn %{"instance" => url} ->
            {:ok, {{_, 200, _}, _headers, body}} =
              :httpc.request(:get, {to_charlist(url), headers}, [], [])

            Cluster.Logger.debug(:gce, "    Received instance data: #{inspect(body)}")

            network_ip =
              body
              |> Jason.decode!()
              |> Map.get("networkInterfaces")
              |> hd
              |> Map.get("networkIP")

            Cluster.Logger.debug(:gce, "    Node network IP is: #{inspect(network_ip)}")

            node_name = :"#{release_name}@#{network_ip}"

            Cluster.Logger.debug(:gce, "   - Found node: #{inspect(node_name)}")

            node_name
          end)
        end

      {:ok, {{_, resp_code, _}, _headers, body}} ->
        Cluster.Logger.error(:gce, "GCP API error: #{resp_code}: #{inspect(body)}")
        []

      {:error, message} ->
        Cluster.Logger.error(:gce, "GCP API error: #{inspect(message)}")
        []
    end
  end

  defp get_region_nodes(state, region, group_name) do
    Cluster.Logger.debug(:gce, "Loading nodes from GCE API...")

    auth_token =
      get_metadata('/instance/service-accounts/default/token')
      |> Jason.decode!()
      |> Map.get("access_token")

    release_name = get_release_name(state)

    headers = [{'Authorization', 'Bearer #{auth_token}'}]

    url = @google_api_base_url ++ '/regions/#{region}/instanceGroups/#{group_name}/listInstances'

    Cluster.Logger.debug(:gce, "Fetching instances from #{inspect(url)}")

    case :httpc.request(:post, {url, headers, 'application/json', ''}, [], []) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        Cluster.Logger.debug(:gce, "    Received body: #{inspect(body)}")

        items =
          Jason.decode!(body)
          |> Map.get("items")

        if is_nil(items) do
          []
        else
          Enum.filter(items, fn
            %{"status" => "RUNNING"} -> true
            _ -> false
          end)
          |> Enum.map(fn %{"instance" => url} ->
            {:ok, {{_, 200, _}, _headers, body}} =
              :httpc.request(:get, {to_charlist(url), headers}, [], [])

            Cluster.Logger.debug(:gce, "    Received instance data: #{inspect(body)}")

            network_ip =
              body
              |> Jason.decode!()
              |> Map.get("networkInterfaces")
              |> hd
              |> Map.get("networkIP")

            Cluster.Logger.debug(:gce, "    Node network IP is: #{inspect(network_ip)}")

            node_name = :"#{release_name}@#{network_ip}"

            Cluster.Logger.debug(:gce, "   - Found node: #{inspect(node_name)}")

            node_name
          end)
        end

      {:ok, {{_, resp_code, _}, _headers, body}} ->
        Cluster.Logger.error(:gce, "GCP API error: #{resp_code}: #{inspect(body)}")
        []

      {:error, message} ->
        Cluster.Logger.error(:gce, "GCP API error: #{inspect(message)}")
        []
    end
  end

  defp get_metadata(path) do
    headers = [{'Metadata-Flavor', 'Google'}]
    url = @metadata_base_url ++ path

    Cluster.Logger.debug(:gce, "Fetching Metadata from #{inspect(url)}...")

    case :httpc.request(:get, {url, headers}, [], []) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        Cluster.Logger.debug(:gce, "    Received body: #{inspect(body)}")
        body
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
