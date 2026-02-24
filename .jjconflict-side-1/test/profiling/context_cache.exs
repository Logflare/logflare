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
str_key = inspect({:get, [user.id]})

Benchee.run(
  %{
    "Cachex.get/3" => fn _ ->
      Cachex.get(Logflare.Users.Cache, {:get, [user.id]})
    end,
    "Logflare.Users.get/1" => fn _ ->
      Logflare.Users.Cache.get(user.id)
    end
  },
  before_scenario: fn input ->
    Cachex.put(Logflare.Users.Cache, {:get, [user.id]}, {:cached, user})
    Cachex.put(Logflare.Users.Cache, str_key, {:cached, user})
    {input, nil}
  end,
  time: 4,
  memory_time: 0
)

# Before 090f8d93:
# Name                               ips        average  deviation         median         99th %
# Cachex.get/3                    1.40 M      714.86 ns  ±2833.04%         625 ns         833 ns
# ContextCache.apply_fun/3        1.03 M      968.97 ns  ±1686.53%         792 ns        1167 ns

# Comparison:
# Cachex.get/3                    1.40 M
# ContextCache.apply_fun/3        1.03 M - 1.36x slower +254.10 ns
