defmodule Logflare.Billing.Cache do
  @moduledoc false

  alias Logflare.Billing
  alias Logflare.Utils

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(180, 10)
           ]
         ]}
    }
  end

  @behaviour Logflare.ContextCache

  @impl Logflare.ContextCache
  def bust_actions(action, id) when is_integer(id) do
    value =
      case action do
        :update -> Billing.get_billing_account(id)
        :delete -> :bust
      end

    {:partial, %{{:get_billing_account_by_user, [id]} => value}}
  end

  def get_billing_account_by_user(user_id) when is_integer(user_id) do
    apply_fun(__ENV__.function, [user_id])
  end

  def get_plan_by_user(user), do: apply_fun(__ENV__.function, [user])
  def get_plan_by(keyword), do: apply_fun(__ENV__.function, [keyword])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Billing, arg1, arg2)
  end
end
