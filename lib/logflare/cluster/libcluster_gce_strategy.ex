defmodule Logflare.Cluster.Strategy.GoogleComputeEngine do
  use GenServer
  use Cluster.Strategy

  alias Cluster.Strategy.State

  @default_polling_interval 10_000
  @metadata_base_url 'http://metadata.google.internal/computeMetadata/v1'
  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @google_api_base_url 'https://compute.googleapis.com/compute/v1/projects/#{@project_id}'
  @default_release_name :node

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

  defp get_nodes(state) do
    Cluster.Logger.debug(:gce, "Loading nodes from GCE API...")

    auth_token =
      get_metadata('/instance/service-accounts/default/token')
      |> Jason.decode!()
      |> Map.get("access_token")

    region = "us-central1"

    group_id =
      get_metadata('/instance/attributes/created-by')
      |> String.Chars.to_string()
      |> String.split("/")
      |> List.last()

    release_name = get_release_name(state)

    headers = [{'Authorization', 'Bearer #{auth_token}'}]

    url = @google_api_base_url ++ '/regions/#{region}/instanceGroups/#{group_id}/listInstances'

    Cluster.Logger.debug(:gce, "Fetching instances from #{inspect(url)}")

    case :httpc.request(:post, {url, headers, 'application/json', ''}, [], []) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        Cluster.Logger.debug(:gce, "    Received body: #{inspect(body)}")

        Jason.decode!(body)
        |> Map.get("items")
        |> Enum.filter(
             fn
               %{"status" => "RUNNING"} -> true
               _ -> false
             end
           )
        |> Enum.map(
             fn %{"instance" => instance} ->
               url = instance
                     |> String.replace("https://www.", "https://compute.")
                     |> to_char_list
               {:ok, {{_, 200, _}, _headers, body}} = :httpc.request( :post, {url, headers, 'application/json', ''}, [], [] )
               Cluster.Logger.debug(:gce, "    Received instance data: #{inspect(body)}")

               network_ip = body
                            |> Jason.decode!
                            |> Map.get("networkInterfaces")
                            |> hd
                            |> Map.get("networkIP")

               Cluster.Logger.debug(:gce, "    Node network IP is: #{inspect(network_ip)}")

               node_name = :"#{release_name}@#{network_ip}"

               Cluster.Logger.debug(:gce, "   - Found node: #{inspect(node_name)}")

               node_name
             end
           )
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
