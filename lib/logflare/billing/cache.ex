defmodule Logflare.Billing.Cache do
  @moduledoc false

  alias Logflare.Billing

  require Logger
  alias Logflare.Cluster.CacheWarmer

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             stats: stats,
             limit: 100_000,
             warmers: [
               CacheWarmer.warmer_spec(__MODULE__)
             ]
           ]
         ]}
    }
  end

  def get_billing_account_by(keyword) do
    apply_fun(__ENV__.function, [keyword])
  end

  def get_plan_by_user(user), do: apply_fun(__ENV__.function, [user])
  def get_plan_by(keyword), do: apply_fun(__ENV__.function, [keyword])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Billing, arg1, arg2)
  end
end
