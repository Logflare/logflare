defmodule Logflare.BillingAccounts.Cache do
  @moduledoc false

  alias Logflare.BillingAccounts

  require Logger

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: true, limit: 10000]]}}
  end

  def get_billing_account_by(keyword) do
    apply_fun(__ENV__.function, [keyword])
  end

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(BillingAccounts, arg1, arg2)
  end
end
