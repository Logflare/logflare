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
    "Plug.RequestId" =>
      {fn {source, conn} ->
         Plug.RequestId.call(conn, {"x-request-id", nil})
       end,
       before_scenario: fn {source, conn} ->
         {source, conn}
       end},
    "LogflareWeb.Plugs.MaybeContentTypeToJson" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.MaybeContentTypeToJson.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
         {source, conn}
       end},
    "OpenApiSpex.Plug.PutApiSpec" =>
      {fn {source, conn} ->
         OpenApiSpex.Plug.PutApiSpec.call(conn, module: LogflareWeb.ApiSpec)
       end,
       before_scenario: fn {source, conn} ->
         {source, conn}
       end},
    "LogflareWeb.Plugs.SetHeaders" =>
      {fn {source, conn} ->
         LogflareWeb.Plugs.SetHeaders.call(conn, [])
       end,
       before_scenario: fn {source, conn} ->
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

# 19 dec 2024:
# ##### With input v1 #####
# Name                                               ips        average  deviation         median         99th %
# OpenApiSpex.Plug.PutApiSpec                    10.93 M      0.0915 μs   ±278.96%      0.0830 μs        0.21 μs
# LogflareWeb.Plugs.MaybeContentTypeToJson        9.86 M       0.101 μs ±24427.81%      0.0420 μs      0.0840 μs
# LogflareWeb.Plugs.SetHeaders                    0.58 M        1.72 μs   ±531.38%        1.67 μs        1.92 μs
# Plug.RequestId                                  0.51 M        1.97 μs   ±695.24%        1.83 μs        2.38 μs

# Comparison:
# OpenApiSpex.Plug.PutApiSpec                    10.93 M
# LogflareWeb.Plugs.MaybeContentTypeToJson        9.86 M - 1.11x slower +0.00998 μs
# LogflareWeb.Plugs.SetHeaders                    0.58 M - 18.83x slower +1.63 μs
# Plug.RequestId                                  0.51 M - 21.50x slower +1.88 μs
