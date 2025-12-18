defmodule Logflare.Billing.Cache do
  @moduledoc false

  alias Logflare.Billing
  alias Logflare.Utils

  require Logger

  def child_spec(_) do
    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 Utils.cache_stats(),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(180, 10)
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
