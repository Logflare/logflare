alias Logflare.Sources
alias Logflare.Users
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
    "pipeline" => fn input ->
      Phoenix.ConnTest.build_conn()
      |> Phoenix.ConnTest.dispatch(
        LogflareWeb.Endpoint,
        :post,
        "/api/logs?source=#{input.token}&api_key=#{user.api_key}",
        %{
          message: "some msg",
          field: "1234",
          testing: 123
        }
      )
    end
  },
  inputs: %{
    "v1" => v1_source,
    "v2" => v2_source
  },
  time: 10,
  memory_time: 0
)

# Before 090f8d93:
# ##### With input v1 #####
# Name               ips        average  deviation         median         99th %
# pipeline         36.05       27.74 ms    ±20.18%       26.72 ms       47.35 ms

# ##### With input v2 #####
# Name               ips        average  deviation         median         99th %
# pipeline         37.07       26.98 ms    ±21.35%       25.97 ms       55.37 ms

# With bandit (2024-12-10):
# ##### With input v1 #####
# Name               ips        average  deviation         median         99th %
# pipeline         44.25       22.60 ms    ±14.53%       21.89 ms       34.32 ms

# ##### With input v2 #####
# Name               ips        average  deviation         median         99th %
# pipeline         45.08       22.18 ms    ±11.26%       22.16 ms       29.60 ms
