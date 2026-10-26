defmodule Logflare.KeyValues.Cache.L2 do
  @moduledoc """
  Distributed (L2) level of `Logflare.KeyValues.Cache`, consulted on an L1 miss before the
  database.

  Keys are partitioned across the cluster by a consistent hash ring; each node holds its
  share in a `Nebulex.Adapters.Local` primary storage, reachable through `__primary__/0`
  without RPC. It is off unless `enabled?/0`, and the settings below live under
  `config :logflare, Logflare.KeyValues.Cache`:

    * `:l2_enabled` - starts the level at boot and gates its use at runtime
    * `:l2_timeout` - RPC timeout in ms; a slower L2 call counts as an error
    * `:l2_ttl` - time-to-live in ms of an entry written to L2

  Adapter options come from `config :logflare, Logflare.KeyValues.Cache.L2`. The defaults:

    * `primary: [compressed: true]` - values are JSON maps, compressed as in L1
    * `primary: [gc_interval: 1h]` - generations rotate hourly, so with the 1h `:l2_ttl`
      expired entries are reclaimed within two hours even if never read again
    * `primary: [max_size: 2_000_000, allocated_memory: 1 GiB]` - caps one node's share;
      crossing either drops the older generation instead of growing the VM
    * `rejoin_interval: 30s` - the adapter default; a missed `:pg` join only routes keys to
      a different owner (a miss), never serves a wrong value, so faster convergence buys little
    * `hash_ring` - `ExHashRing` defaults (512 vnodes per node)
  """

  use Nebulex.Cache,
    otp_app: :logflare,
    adapter: Nebulex.Adapters.Partitioned

  @spec enabled?() :: boolean()
  def enabled?, do: Keyword.get(settings(), :l2_enabled, false)

  @spec timeout() :: timeout()
  def timeout, do: Keyword.get(settings(), :l2_timeout, 50)

  @spec ttl() :: timeout()
  def ttl, do: Keyword.get(settings(), :l2_ttl, to_timeout(hour: 1))

  @doc """
  Classifies the result of an L2 command for telemetry.

  `:hit` and `:miss` are fetch results; `:timeout` is an RPC that ran out of `timeout/0`.
  """
  @spec outcome(term()) :: atom()
  def outcome({:ok, _value}), do: :hit
  def outcome({:error, %Nebulex.KeyError{}}), do: :miss
  def outcome({:error, %Nebulex.Error{reason: {:rpc, {:error, :timeout}}}}), do: :timeout
  def outcome({:error, _reason}), do: :error
  def outcome(result) when is_atom(result), do: result

  defp settings, do: Application.get_env(:logflare, Logflare.KeyValues.Cache, [])
end
