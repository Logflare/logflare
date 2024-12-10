alias Logflare.Sources
alias Logflare.Users
alias Logflare.Partners
require Phoenix.ConnTest
Mimic.copy(Broadway)
Mimic.copy(Logflare.Backends)
Mimic.copy(Logflare.Logs)
Mimic.copy(Logflare.Partners)

Mimic.stub(Logflare.Backends, :ingest_logs, fn _, _ -> :ok end)
Mimic.stub(Logflare.Logs, :ingest_logs, fn _, _ -> :ok end)
# Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)
ver = System.argv() |> Enum.at(0)

v1_source = Sources.get(:"9f37d86e-e4fa-4ef2-a47e-e8d4ac1fceba")

v2_source = Sources.get(:"94d07aab-30f5-460e-8871-eb85f4674e35")

user = Users.get(v1_source.user_id)

Benchee.run(
  %{
    "VerifyApiAccess" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.VerifyApiAccess.call(conn, scopes: ~w(public))
       end,
       before_scenario: fn {source, conn} ->
         {source, conn}
       end},
    "FetchResource" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.FetchResource.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         conn =
           conn
           |> LogflareWeb.Plugs.VerifyApiAccess.call(scopes: ~w(public))

         {source, conn}
       end},
    "VerifyResourceOwnership" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.VerifyResourceOwnership.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         conn =
           conn
           |> LogflareWeb.Plugs.VerifyApiAccess.call(scopes: ~w(public))
           |> LogflareWeb.Plugs.FetchResource.call([])

         {source, conn}
       end},
    "SetPlanFromCache" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.SetPlanFromCache.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         conn =
           conn
           |> LogflareWeb.Plugs.VerifyApiAccess.call(scopes: ~w(public))
           |> LogflareWeb.Plugs.FetchResource.call([])
           |> LogflareWeb.Plugs.VerifyResourceOwnership.call([])

         {source, conn}
       end},
    "RateLimiter" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.RateLimiter.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         conn =
           conn
           |> LogflareWeb.Plugs.VerifyApiAccess.call(scopes: ~w(public))
           |> LogflareWeb.Plugs.FetchResource.call([])
           |> LogflareWeb.Plugs.VerifyResourceOwnership.call([])
           |> LogflareWeb.Plugs.SetPlanFromCache.call([])

         {source, conn}
       end},
    "BufferLimiter" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.BufferLimiter.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         conn =
           conn
           |> LogflareWeb.Plugs.VerifyApiAccess.call(scopes: ~w(public))
           |> LogflareWeb.Plugs.FetchResource.call([])
           |> LogflareWeb.Plugs.VerifyResourceOwnership.call([])
           |> LogflareWeb.Plugs.SetPlanFromCache.call([])
           |> LogflareWeb.Plugs.RateLimiter.call([])

         {source, conn}
       end}
  },
  inputs: %{
    "v1" => v1_source
    # "v2" => v2_source
  },
  before_scenario: fn input ->
    prepared_conn =
      Phoenix.ConnTest.build_conn(
        :post,
        "/api/logs?source=#{input.token}&api_key=#{user.api_key}",
        %{
          message: "some msg",
          field: "1234",
          testing: 123
        }
      )
      |> Plug.Conn.assign(:resource_type, :source)

    {input, prepared_conn}
  end,
  time: 4,
  memory_time: 0
)

# Before 090f8d93:
# ##### With input v1 #####
# Name                              ips        average  deviation         median         99th %
# VerifyResourceOwnership    14120.42 K      0.0708 μs ±25384.39%      0.0830 μs      0.0840 μs
# FetchResource                668.87 K        1.50 μs  ±1270.86%        1.33 μs        2.58 μs
# BufferLimiter                332.39 K        3.01 μs   ±428.79%        2.79 μs        4.67 μs
# VerifyApiAccess              111.55 K        8.96 μs   ±168.32%        7.46 μs       27.17 μs
# RateLimiter                   50.82 K       19.68 μs    ±23.81%       19.17 μs       26.04 μs
# SetPlanFromCache              36.09 K       27.71 μs    ±22.85%       25.29 μs       40.29 μs

# Comparison:
# VerifyResourceOwnership    14120.42 K
# FetchResource                668.87 K - 21.11x slower +1.42 μs
# BufferLimiter                332.39 K - 42.48x slower +2.94 μs
# VerifyApiAccess              111.55 K - 126.58x slower +8.89 μs
# RateLimiter                   50.82 K - 277.83x slower +19.61 μs
# SetPlanFromCache              36.09 K - 391.23x slower +27.64 μs
