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


apply_args = {:get, [user.id]}

Benchee.run(
  %{
    "Cachex.fetch/3" => fn ->
      Cachex.fetch(Logflare.Users.Cache, apply_args, fn {fun, args} ->
        value = apply(Logflare.Users, :get, [user.id])
        # keys_key = {{Logflare.Users, Logflare.ContextCache.select_key(value)}, :erlang.phash2(apply_args)}
        # Cachex.put(Logflare.ContextCache, keys_key, apply_args)

        {:commit, {:cached, value}}
      end)
    end,
    "Users.Cache.get/1" => fn ->
      Logflare.Users.Cache.get(user.id)
    end,
  },
  before_each: fn input ->
    Cachex.clear(Logflare.Users.Cache)
    Cachex.clear(Logflare.ContextCache)
    input
  end,
  time: 4,
  memory_time: 0
)

# Before 090f8d93:
# Name                        ips        average  deviation         median         99th %
# Users.Cache.get/1        1.67 K      598.09 μs    ±69.70%      546.42 μs     1175.40 μs
# Cachex.fetch/3           1.41 K      707.75 μs    ±38.63%      664.25 μs     1510.41 μs

# Comparison:
# Users.Cache.get/1        1.67 K
# Cachex.fetch/3           1.41 K - 1.18x slower +109.67 μs
