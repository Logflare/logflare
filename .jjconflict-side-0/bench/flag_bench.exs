# Usage: mix run bench/flag_bench.exs
#
# Compares throughput of cached (binary identifier) vs uncached (User) flag lookups.

alias Logflare.ConfigCatCache
alias Logflare.User
alias Logflare.Utils

ConfigCatCache
|> Supervisor.child_spec([])
|> then(&Supervisor.start_link([&1], strategy: :one_for_one))

Application.put_env(:logflare, :env, :prod)
Application.put_env(:logflare, :config_cat_sdk_key, "bench-sdk-key")

# Stub ConfigCat to return a fixed value without network calls.
Mimic.copy(ConfigCat)
Mimic.stub(ConfigCat, :get_value, fn _feature, _default, _user -> true end)

user = %User{email: "bench@example.com"}

# Warm the cache for binary identifier path
Utils.flag("bench_feature", "bench-id")

Benchee.run(
  %{
    "binary identifier (cached)" => fn -> Utils.flag("bench_feature", "bench-id") end,
    "User (uncached)" => fn -> Utils.flag("bench_feature", user) end
  },
  time: 2,
  warmup: 1
)
