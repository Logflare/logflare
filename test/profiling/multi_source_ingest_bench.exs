alias Logflare.Sources
alias Logflare.Users
require Phoenix.ConnTest
Mimic.copy(Broadway)
Mimic.copy(Logflare.Backends)
Mimic.copy(Logflare.Logs)
Mimic.copy(Logflare.Partners)

Mimic.stub(Logflare.Backends, :ingest_logs, fn _, _ -> :ok end)
Mimic.stub(Logflare.Logs, :ingest_logs, fn _, _ -> :ok end)

v1_source = Sources.get(:"9f37d86e-e4fa-4ef2-a47e-e8d4ac1fceba")
v2_source = Sources.get(:"94d07aab-30f5-460e-8871-eb85f4674e35")

user = Users.get(v1_source.user_id)

token_a = Atom.to_string(v1_source.token)
token_b = Atom.to_string(v2_source.token)

build_single_source_conn = fn ->
  Phoenix.ConnTest.build_conn(
    :post,
    "/api/logs?source=#{token_a}&api_key=#{user.api_key}",
    %{"batch" => for(_ <- 1..10, do: %{message: "some msg", field: "1234"})}
  )
  |> Plug.Conn.assign(:resource_type, :source)
  |> Plug.Conn.assign(:user, user)
  |> Plug.Conn.assign(:source, v1_source)
end

build_multi_source_conn = fn ->
  events =
    for i <- 1..10 do
      token = if rem(i, 2) == 0, do: token_a, else: token_b
      %{"__LF_SOURCE" => token, "message" => "some msg #{i}"}
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

Benchee.run(
  %{
    "VerifyDeclaredSources - passthrough (no __LF_SOURCE)" =>
      {fn conn ->
         LogflareWeb.Plugs.VerifyDeclaredSources.call(conn, [])
       end,
       before_each: fn _ -> build_single_source_conn.() end},
    "VerifyDeclaredSources - multi-source (2 sources, 10 events)" =>
      {fn conn ->
         LogflareWeb.Plugs.VerifyDeclaredSources.call(conn, [])
       end,
       before_each: fn _ -> build_multi_source_conn.() end},
    "Full ingest pipeline - single source (baseline)" =>
      {fn _ ->
         Phoenix.ConnTest.build_conn()
         |> Phoenix.ConnTest.dispatch(
           LogflareWeb.Endpoint,
           :post,
           "/api/logs?source=#{token_a}&api_key=#{user.api_key}",
           %{"batch" => for(_ <- 1..10, do: %{message: "some msg"})}
         )
       end, before_each: fn _ -> nil end},
    "Full ingest pipeline - multi source (2 declared sources)" =>
      {fn _ ->
         events =
           for i <- 1..10 do
             token = if rem(i, 2) == 0, do: token_a, else: token_b
             %{"__LF_SOURCE" => token, "message" => "some msg #{i}"}
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
  inputs: %{"default" => nil},
  time: 4,
  memory_time: 0
)
