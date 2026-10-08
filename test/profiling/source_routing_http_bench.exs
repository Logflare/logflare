System.put_env("ROUTING_BENCH_LIBRARY", "1")

Code.require_file(
  System.get_env(
    "ROUTING_BENCH_LIBRARY_PATH",
    Path.join(__DIR__, "source_routing_scale_bench.exs")
  )
)

defmodule RoutingHttpBench do
  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Factory
  alias Logflare.Repo
  alias Logflare.Sources.SourceRouter.RulesTree
  alias LogflareWeb.Endpoint

  @spec run() :: :ok
  def run do
    Code.ensure_loaded!(RulesTree)
    Logger.configure(level: :warning)
    :ok = Sandbox.checkout(Repo)
    :ok = Sandbox.mode(Repo, {:shared, self()})
    configure()

    if Repo.get_by(Logflare.Billing.Plan, name: "Free") == nil do
      Factory.insert(:plan, limit_rate_limit: 1_000_000, limit_source_rate_limit: 1_000_000)
    end

    user = Factory.insert(:user)

    fixtures =
      Enum.map(RoutingScaleBench.integers("ROUTING_BENCH_RULES", "100,10000"), &fixture(&1, user))

    IO.write(
      "ROUTING_HTTP_TABLES " <>
        Jason.encode!(
          Enum.flat_map(fixtures, fn fixture ->
            Enum.map([fixture.source, fixture.sink], &PostgresAdaptor.table_name/1)
          end)
        )
    )

    try do
      health = Req.get!("http://127.0.0.1:4143/health/", retry: false)
      200 = health.status

      rows =
        for fixture <- fixtures,
            batch <- RoutingScaleBench.integers("ROUTING_BENCH_BATCHES", "1,10,100") do
          measure(fixture, batch, user)
        end

      result = %{
        revision: System.fetch_env!("ROUTING_MAIN_REVISION"),
        system: %{
          elixir: System.version(),
          otp: System.otp_release(),
          schedulers: :erlang.system_info(:schedulers_online)
        },
        endpoint: "http://127.0.0.1:4143",
        spool: "disabled",
        backend: "actual PostgreSQL default backend",
        results: rows
      }

      File.write!(System.fetch_env!("ROUTING_BENCH_OUTPUT"), Jason.encode!(result, pretty: true))
    after
      cleanup(fixtures, user)
      :ok = Sandbox.checkin(Repo)
    end
  end

  @spec cleanup(list(), struct()) :: :ok
  defp cleanup(fixtures, user) do
    for fixture <- fixtures, source <- [fixture.source, fixture.sink] do
      Backends.stop_source_sup(source)

      SharedRepo.with_repo(Backends.get_default_backend(user), fn ->
        SharedRepo.query!("DROP TABLE IF EXISTS \"#{PostgresAdaptor.table_name(source)}\"", [])
      end)
    end

    :ok
  end

  @spec configure() :: :ok
  defp configure do
    repo = Application.fetch_env!(:logflare, Repo)

    url =
      "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

    Application.put_env(:logflare, :single_tenant, true)
    Application.put_env(:logflare, :supabase_mode, false)

    Application.put_env(:logflare, :postgres_backend_adapter,
      url: url,
      schema: nil,
      pool_size: 10
    )

    Application.put_env(:logflare, :spool, mode: :disable)

    if Process.whereis(Logflare.SystemMetrics.AllLogsLogged) == nil do
      {:ok, _pid} =
        Supervisor.start_child(Logflare.Supervisor, Logflare.SystemMetrics.AllLogsLogged)
    end

    config = Application.fetch_env!(:logflare, Endpoint)

    Application.put_env(
      :logflare,
      Endpoint,
      Keyword.merge(config, server: true, http: [ip: {127, 0, 0, 1}, port: 4143])
    )

    :ok = Supervisor.terminate_child(Logflare.Supervisor, Endpoint)
    {:ok, _pid} = Supervisor.restart_child(Logflare.Supervisor, Endpoint)
    :ok
  end

  @spec fixture(non_neg_integer(), struct()) :: map()
  defp fixture(count, user) do
    source = Factory.insert(:source, user: user)
    sink = Factory.insert(:source, user: user)
    Backends.ensure_source_sup_started(source)
    Backends.ensure_source_sup_started(sink)
    fixture = RoutingScaleBench.fixture(count, :one)

    rules =
      Enum.map(fixture.rules, &%{&1 | source_id: source.id, backend_id: nil, sink: sink.token})

    %{fixture | source: source, rules: rules, event: %{fixture.event | source_id: source.id}}
    |> Map.put(:sink, sink)
  end

  @spec measure(map(), pos_integer(), struct()) :: map()
  defp measure(fixture, batch, user) do
    RoutingScaleBench.warm(fixture)
    samples = String.to_integer(System.get_env("ROUTING_HTTP_SAMPLES", "30"))
    run_key = "routing-http-" <> Ecto.UUID.generate()
    url = "http://127.0.0.1:4143/logs?source=#{fixture.source.token}"
    headers = [{"x-api-key", user.api_key}, {"content-type", "application/json"}]

    payload = fn ->
      events =
        for _ <- 1..batch,
            do: %{
              "id" => Ecto.UUID.generate(),
              "event_message" => run_key,
              "metadata" => %{
                "rule_id" => "rule-#{min(fixture.count, 100)}",
                "type" => "otel_log"
              },
              "severity_number" => 9
            }

      if batch == 1, do: hd(events), else: %{"batch" => events}
    end

    for _ <- 1..5, do: request(url, headers, payload.())
    backend = Backends.get_default_backend(user)

    wait_counts(
      backend,
      fixture,
      run_key,
      batch * 5,
      System.monotonic_time(:millisecond) + 30_000
    )

    total = batch * (samples + 5)
    started = System.monotonic_time(:microsecond)

    timings =
      for _ <- 1..samples do
        body = payload.()
        {us, :ok} = :timer.tc(fn -> request(url, headers, body) end)
        us
      end

    wait_counts(backend, fixture, run_key, total, System.monotonic_time(:millisecond) + 30_000)
    completion_us = System.monotonic_time(:microsecond) - started
    sorted = Enum.sort(timings)

    %{
      rules: fixture.count,
      batch: batch,
      samples: samples,
      mean_us: Enum.sum(timings) / samples,
      median_us: Enum.at(sorted, div(samples, 2)),
      p99_us: List.last(sorted),
      request_us: timings,
      requests_and_backend_completion_us: completion_us,
      accepted_events: total,
      primary_rows: total,
      sink_rows: total
    }
  end

  @spec request(binary(), list(), map()) :: :ok
  defp request(url, headers, payload) do
    response =
      Req.post!(url, headers: headers, json: payload, retry: false, receive_timeout: 30_000)

    if response.status != 200,
      do: raise("HTTP ingestion rejected: #{inspect(response.status)} #{inspect(response.body)}")

    :ok
  end

  @spec wait_counts(struct(), map(), binary(), pos_integer(), integer()) :: :ok
  defp wait_counts(backend, fixture, run_key, expected, deadline) do
    counts =
      SharedRepo.with_repo(backend, fn ->
        for source <- [fixture.source, fixture.sink] do
          %{rows: [[count, distinct]]} =
            SharedRepo.query!(
              "SELECT count(*), count(DISTINCT id) FROM \"#{PostgresAdaptor.table_name(source)}\" WHERE event_message = $1",
              [run_key]
            )

          {count, distinct}
        end
      end)

    cond do
      counts == [{expected, expected}, {expected, expected}] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise("Backend counts #{inspect(counts)} expected #{expected}")

      true ->
        receive do
        after
          20 -> :ok
        end

        wait_counts(backend, fixture, run_key, expected, deadline)
    end
  end
end

RoutingHttpBench.run()
