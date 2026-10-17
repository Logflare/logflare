defmodule Logflare.Billing.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.User

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 3),
      purge_interval: to_timeout(minute: 10)
    )
  end

  def get_billing_account_by(keyword) do
    apply_fun(__ENV__.function, [keyword])
  end

  @spec get_plan_by_user(User.t()) :: Plan.t()
  def get_plan_by_user(%User{id: id} = user) do
    fetch({:get_plan_by_user, [id]}, fn ->
      Repo.apply_with_replica(Billing, :get_plan_by_user, [user])
    end)
  end

  def get_plan_by(keyword), do: apply_fun(__ENV__.function, [keyword])

  @impl ContextCache
  def keys_to_bust(user_id: id) do
    [{:get_plan_by_user, [id]}, {:get_billing_account_by, [[user_id: id]]}]
  end

  def keys_to_bust(id: _id, user_id: user_id), do: keys_to_bust(user_id: user_id)

  def keys_to_bust(billing_accounts: :changed) do
    filter =
      {:orelse, {:==, {:element, 1, :key}, :get_plan_by_user},
       {:==, {:element, 1, :key}, :get_billing_account_by}}

    query = Cachex.Query.build(where: filter, output: :key)
    Cachex.stream!(__MODULE__, query)
  end

  def keys_to_bust(kw), do: CachexOps.keys_to_bust(__MODULE__, kw)

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Billing, arg1, arg2)
  end
end
