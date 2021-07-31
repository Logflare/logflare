defmodule Logflare.BillingAccounts.Cache do
  @moduledoc false
  import Cachex.Spec

  alias Logflare.BillingAccounts

  require Logger

  @cache __MODULE__

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  def get_billing_account_by(keyword) do
    apply_fun(__ENV__.function, [keyword])
  end

  def get_billing_account_stripe_plan(billing_account),
    do: apply_fun(__ENV__.function, [billing_account])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(BillingAccounts, arg1, arg2)
  end
end
