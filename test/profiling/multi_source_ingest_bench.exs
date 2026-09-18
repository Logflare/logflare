# VerifyDeclaredSources plug, then full HTTP→ingest (two Benchee runs, sequential).
# Usage: MIX_ENV=test mix run test/profiling/multi_source_ingest_bench.exs

import Logflare.Factory

require Phoenix.ConnTest

{:ok, _} = Application.ensure_all_started(:mimic)

Mimic.copy(Broadway)
Mimic.copy(Logflare.Backends)
Mimic.copy(Goth)
Mimic.copy(Logflare.Partners)
Mimic.copy(Logflare.Sources.Source.Data)

# RateCounterServer boots in SourceSup and calls Goth / Data.get_log_count; private-mode
# Mimic stubs would not apply. Global mode mirrors ExUnit setups that stub GenServers.
Mimic.set_mimic_global()

Mimic.stub(Goth)

Mimic.stub(Logflare.Sources.Source.Data, :get_log_count, fn _, _ ->
  0
end)

Mimic.stub(Logflare.Backends, :ingest_logs, fn events, _ ->
  {:ok, length(events)}
end)

_pid =
  Ecto.Adapters.SQL.Sandbox.start_owner!(Logflare.Repo,
    shared: true,
    ownership_timeout: 1_200_000
  )

insert(:plan)

user = insert(:user)
v1_source = insert(:source, user: user)

token_a = Atom.to_string(v1_source.token)
batch_size = 500

build_single_source_conn = fn ->
  Phoenix.ConnTest.build_conn(
    :post,
    "/api/logs?source=#{token_a}&api_key=#{user.api_key}",
    %{"batch" => for(_ <- 1..batch_size, do: %{message: "some msg", field: "1234"})}
  )
  |> Plug.Conn.assign(:resource_type, :source)
  |> Plug.Conn.assign(:user, user)
  |> Plug.Conn.assign(:source, v1_source)
end

# One unique __LF_SOURCE per batch so VerifyDeclaredSources calls
# VerifyResourceAccess.verify_source_access/3 once (not once per distinct token).
build_multi_source_conn = fn ->
  events =
    for i <- 1..batch_size do
      %{"__LF_SOURCE" => token_a, "message" => "some msg #{i}"}
    end

  Phoenix.ConnTest.build_conn(
    :post,
    "/api/logs?api_key=#{user.api_key}",
    %{"batch" => events}
  )
  |> Plug.Conn.assign(:resource_type, :source)
  |> Plug.Conn.assign(:user, user)
  |> Plug.Conn.assign(:source, nil)
end

bench_opts = [inputs: %{"default" => nil}, time: 6, reduction_time: 4, memory_time: 0]

Benchee.run(
  %{
    "VerifyDeclaredSources - passthrough (no __LF_SOURCE)" =>
      {fn conn ->
         LogflareWeb.Plugs.VerifyDeclaredSources.call(conn, [])
       end, before_each: fn _ -> build_single_source_conn.() end},
    "VerifyDeclaredSources - multi-source (1 source, 500 events)" =>
      {fn conn ->
         LogflareWeb.Plugs.VerifyDeclaredSources.call(conn, [])
       end, before_each: fn _ -> build_multi_source_conn.() end}
  },
  bench_opts
)

Benchee.run(
  %{
    "Full ingest pipeline - single source (baseline)" =>
      {fn _ ->
         Phoenix.ConnTest.build_conn()
         |> Phoenix.ConnTest.dispatch(
           LogflareWeb.Endpoint,
           :post,
           "/api/logs?source=#{token_a}&api_key=#{user.api_key}",
           %{"batch" => for(_ <- 1..batch_size, do: %{message: "some msg"})}
         )
       end, before_each: fn _ -> nil end},
    "Full ingest pipeline - multi source (1 declared source)" =>
      {fn _ ->
         events =
           for i <- 1..batch_size do
             %{"__LF_SOURCE" => token_a, "message" => "some msg #{i}"}
           end

         Phoenix.ConnTest.build_conn()
         |> Phoenix.ConnTest.dispatch(
           LogflareWeb.Endpoint,
           :post,
           "/api/logs?api_key=#{user.api_key}",
           %{"batch" => events}
         )
       end, before_each: fn _ -> nil end}
  },
  bench_opts
)
