defmodule Logflare.CacheWarmerReplicaTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Rules
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Users

  test "cache warmers use scoped replica routing" do
    ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :replica_route]])
    on_exit(fn -> :telemetry.detach(ref) end)

    warmers = [
      Backends.CacheWarmer,
      Users.CacheWarmer,
      Sources.CacheWarmer,
      Rules.CacheWarmer,
      SourceSchemas.CacheWarmer
    ]

    for warmer <- warmers do
      assert {:ok, _entries} = warmer.execute(nil)

      assert_receive {[:logflare, :repo, :replica_route], ^ref, %{count: 1},
                      %{role: :primary, reason: :not_configured}}
    end
  end
end
