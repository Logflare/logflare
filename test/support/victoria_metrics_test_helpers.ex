defmodule Logflare.VictoriaMetricsTestHelpers do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks
  import Logflare.Factory
  import Mimic

  require Logflare.TestUtils

  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.Logs.Processor
  alias Logflare.Logs.Raw
  alias Logflare.Sources.Source
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtils

  @vm_retry [sleep: 250, duration: 30_000]
  @drop_event [:logflare, :backends, :victoria_metrics, :drop]

  @spec ensure_vm_available(map()) :: :ok
  def ensure_vm_available(_context) do
    url = vm_base_url() <> "/health"
    response = HTTPoison.get(url, [], timeout: 1_000, recv_timeout: 1_000)

    assert match?({:ok, %{status_code: 200}}, response),
           """
           VictoriaMetrics is unavailable at #{url}.
           Start it with: docker compose up -d vm
           For a custom instance, set VICTORIA_METRICS_URL to its base URL.
           Health check returned: #{inspect(response, limit: 5, printable_limit: 200)}
           """

    :ok
  end

  @spec setup_vm(map()) :: keyword()
  def setup_vm(context) do
    stub(Logflare.Utils.SSRF, :safe_resolve, fn
      host when host in ["localhost", "127.0.0.1"] -> {:ok, {127, 0, 0, 1}}
      host -> Mimic.call_original(Logflare.Utils.SSRF, :safe_resolve, [host])
    end)

    vm_uri = URI.parse(vm_base_url())

    stub(Finch, :request, fn request, pool, opts ->
      if request.port == vm_uri.port and
           (request.host == vm_uri.host or {"host", vm_uri.host} in request.headers) do
        Mimic.call_original(Finch, :request, [request, pool, opts])
      else
        {:error, :unexpected_external_request}
      end
    end)

    stub(Logflare.Google.BigQuery, :get_table, fn _ -> {:error, :not_found} end)
    start_supervised!(AllLogsLogged)
    insert(:plan)
    source = insert(:source, user: insert(:user), name: "vm_#{Ecto.UUID.generate()}")

    backend =
      insert(:backend,
        type: :victoria_metrics,
        user: source.user,
        sources: [source],
        config: %{
          url: vm_base_url() <> "/api/v1/write",
          labels: context[:config_labels] || %{}
        }
      )

    TestUtils.attach_forwarder(@drop_event)
    start_supervised!({SourceSup, source})
    [source: source, backend: backend]
  end

  @spec vm_base_url() :: String.t()
  def vm_base_url, do: System.get_env("VICTORIA_METRICS_URL", "http://localhost:8428")

  @spec metric_params(keyword()) :: map()
  def metric_params(attrs) do
    defaults = %{
      "event_message" => "metric",
      "metadata" => %{"type" => "metric"},
      "timestamp" => System.system_time(:nanosecond)
    }

    Map.merge(defaults, Map.new(attrs, fn {key, value} -> {Atom.to_string(key), value} end))
  end

  @spec submit([map()], Source.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def submit(params, source), do: Processor.ingest(params, Raw, source)

  @spec ingest([map()], Source.t(), pos_integer()) :: [map()]
  def ingest(params, source, count \\ 1) do
    assert {:ok, _} = submit(params, source)
    wait_for_series(source, count)
  end

  @spec wait_for_series(Source.t(), pos_integer()) :: [map()]
  def wait_for_series(source, count) do
    TestUtils.retry_assert(@vm_retry, fn ->
      series = export(source)
      assert length(series) == count
      series
    end)
  end

  @spec assert_drops(Backend.t(), %{atom() => pos_integer()}) :: :ok
  def assert_drops(%Backend{id: backend_id}, expected) do
    for {reason, count} <- expected do
      receive_drops(backend_id, reason, count)
    end

    :ok
  end

  @spec export(Source.t()) :: [map()]
  def export(source) do
    params =
      URI.encode_query(%{
        "match[]" => "{source=\"#{source.name}\"}",
        "reduce_mem_usage" => 1
      })

    assert {:ok, %{status_code: 200, body: body}} =
             HTTPoison.get(vm_base_url() <> "/api/v1/export?" <> params)

    body
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
    |> Enum.group_by(& &1["metric"])
    |> Enum.map(fn {metric, blocks} ->
      samples =
        blocks
        |> Enum.flat_map(fn block -> Enum.zip(block["timestamps"], block["values"]) end)
        |> Enum.sort_by(&elem(&1, 0))

      {timestamps, values} = Enum.unzip(samples)
      %{"metric" => metric, "timestamps" => timestamps, "values" => values}
    end)
  end

  @spec buckets([map()], String.t()) :: map()
  def buckets(series, name) do
    for %{"metric" => %{"__name__" => ^name, "le" => bound}, "values" => values} <- series,
        into: %{},
        do: {bound, values}
  end

  @spec values_by_name([map()]) :: map()
  def values_by_name(series) do
    Map.new(series, &{&1["metric"]["__name__"], &1["values"]})
  end

  @spec receive_drops(integer(), atom(), non_neg_integer()) :: :ok
  defp receive_drops(_backend_id, _reason, 0), do: :ok

  defp receive_drops(backend_id, reason, remaining) do
    assert_receive {:telemetry_event, @drop_event, %{count: count},
                    %{backend_id: ^backend_id, reason: ^reason}},
                   5_000

    assert count <= remaining
    receive_drops(backend_id, reason, remaining - count)
  end
end
