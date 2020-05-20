defmodule Logflare.BillingAccounts.Cache do
  @moduledoc false
  import Cachex.Spec

  alias Logflare.BillingAccounts

  @ttl 5_000

  @cache __MODULE__

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          @cache,
          [expiration: expiration(default: @ttl)]
        ]
      }
    }
  end

  def get_billing_account_by(keyword), do: apply_fun(__ENV__.function, [keyword])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(BillingAccounts, arg1, arg2)
  end
end
