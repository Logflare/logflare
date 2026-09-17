defmodule Logflare.Billing do
  @moduledoc false

  import Ecto.Query, warn: false

  alias __MODULE__

  alias Logflare.Billing.BillingAccount
  alias Logflare.Billing.PaymentMethod
  alias Logflare.Billing.Plan
  alias Logflare.Partners
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source
  alias Logflare.User
  alias Logflare.Users

  require Logger
  require Protocol
  Protocol.derive(Jason.Encoder, Stripe.List)
  Protocol.derive(Jason.Encoder, Stripe.Subscription)
  Protocol.derive(Jason.Encoder, Stripe.Plan)
  Protocol.derive(Jason.Encoder, Stripe.SubscriptionItem)
  Protocol.derive(Jason.Encoder, Stripe.Checkout.Session)
  Protocol.derive(Jason.Encoder, Stripe.Invoice)
  Protocol.derive(Jason.Encoder, Stripe.LineItem)
  Protocol.derive(Jason.Encoder, Stripe.Price)
  Protocol.derive(Jason.Encoder, Stripe.Coupon)

  # BillingAccount

  @doc "Returns the list of billing_accounts"
  @spec list_billing_accounts() :: [BillingAccount.t()]
  def list_billing_accounts, do: Repo.all(BillingAccount)

  @doc "Gets a single billing_account by a keyword."
  @spec get_billing_account_by(keyword()) :: BillingAccount.t() | nil
  def get_billing_account_by(kv), do: Repo.get_by(BillingAccount, kv)

  @doc "Gets a single billing_account. Raises `Ecto.NoResultsError` if the Billing account does not exist."
  @spec get_billing_account!(String.t() | number()) :: BillingAccount.t()
  def get_billing_account!(id), do: Repo.get!(BillingAccount, id)

  @doc "Creates a billing_account."
  @spec create_billing_account(User.t(), map()) ::
          {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def create_billing_account(%User{} = user, attrs \\ %{}) do
    user
    |> Ecto.build_assoc(:billing_account)
    |> BillingAccount.changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _user} = res ->
        # move this to be the default on user create after launch
        Users.update_user_all_fields(user, %{billing_enabled: true})

        Source.Supervisor.reset_all_user_sources(user)
        res

      {:error, _changeset} = res ->
        res
    end
  end

  @doc "Syncs stripe subscription with %BillingAccount{} with Stripe as source of truth."
  @spec sync_subscriptions(nil | BillingAccount.t()) ::
          :noop | {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def sync_subscriptions(nil), do: :noop

  def sync_subscriptions(%BillingAccount{stripe_customer: stripe_customer_id} = billing_account) do
    with {:ok, subscriptions} <-
           Billing.Stripe.list_customer_subscriptions(stripe_customer_id) do
      update_billing_account(billing_account, %{stripe_subscriptions: subscriptions})
    end
  end

  @doc "Syncs stripe invoices with %BillingAccount{} with Stripe as source of truth."
  @spec sync_invoices(nil | BillingAccount.t()) ::
          :noop | {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def sync_invoices(nil), do: :noop

  def sync_invoices(%BillingAccount{stripe_customer: stripe_customer_id} = billing_account) do
    with {:ok, invoices} <- Billing.Stripe.list_customer_invoices(stripe_customer_id) do
      attrs = %{stripe_invoices: invoices}

      update_billing_account(billing_account, attrs)
    end
  end

  @doc "Syncs stripe data with %BillingAccount{} with Stripe as source of truth."
  @spec sync_billing_account(nil | BillingAccount.t()) ::
          :noop | {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def sync_billing_account(
        %BillingAccount{stripe_customer: customer_id} = billing_account,
        attrs \\ %{}
      ) do
    with {:ok, subscriptions} <- Billing.Stripe.list_customer_subscriptions(customer_id),
         {:ok, invoices} <- Billing.Stripe.list_customer_invoices(customer_id),
         {:ok, customer} <- Billing.Stripe.retrieve_customer(customer_id) do
      attrs =
        attrs
        |> Map.put(:stripe_subscriptions, subscriptions)
        |> Map.put(:stripe_invoices, invoices)
        |> Map.put(:default_payment_method, customer.invoice_settings.default_payment_method)
        |> Map.put(:custom_invoice_fields, customer.invoice_settings.custom_fields)

      update_billing_account(billing_account, attrs)
    end
  end

  @doc "Updates a billing_account"
  @spec update_billing_account(BillingAccount.t(), map()) ::
          {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def update_billing_account(%BillingAccount{} = billing_account, attrs) do
    billing_account
    |> BillingAccount.changeset(attrs)
    |> Repo.update()
  end

  @doc "Preloads the payment methods field"
  @spec preload_payment_methods(BillingAccount.t()) :: BillingAccount.t()
  def preload_payment_methods(ba), do: Repo.preload(ba, :payment_methods)

  @doc "Deletes a BillingAccount for a User"
  @spec delete_billing_account(User.t()) ::
          {:ok, BillingAccount.t()} | {:error, Ecto.Changeset.t()}
  def delete_billing_account(%User{billing_account: billing_account} = user) do
    with {:ok, _} = res <- Repo.delete(billing_account) do
      Source.Supervisor.reset_all_user_sources(user)
      res
    end
  end

  @doc "Returns an `%Ecto.Changeset{}` for tracking billing_account changes."
  @spec change_billing_account(BillingAccount.t()) :: Ecto.Changeset.t()
  def change_billing_account(%BillingAccount{} = billing_account) do
    BillingAccount.changeset(billing_account, %{})
  end

  @doc "retrieves the stripe plan stored on the BillingAccount"
  @spec get_billing_account_stripe_plan(BillingAccount.t()) :: nil | map()
  def get_billing_account_stripe_plan(%BillingAccount{
        stripe_subscriptions: %{
          "data" => [%{"items" => %{"data" => [%{"plan" => plan} | _]}} | _]
        }
      }),
      do: plan

  def get_billing_account_stripe_plan(%BillingAccount{
        stripe_subscriptions: %{"data" => [%{"plan" => plan} | _]}
      }),
      do: plan

  def get_billing_account_stripe_plan(_), do: nil

  @doc "gets the stripe subscription item data stored on the BillingAccount"
  @spec get_billing_account_stripe_subscription_item(BillingAccount.t()) :: nil | map()
  def get_billing_account_stripe_subscription_item(%BillingAccount{
        stripe_subscriptions: %{
          "data" => [
            # get first sub
            %{
              "items" => %{
                "data" => [
                  # get first sub item
                  item | _
                ]
              }
            }
            | _
          ]
        }
      }),
      do: item

  def get_billing_account_stripe_subscription_item(_), do: nil

  # PaymentMethod

  @doc "list PaymentMethod by keyword"
  @spec list_payment_methods_by(keyword()) :: [PaymentMethod.t()]
  def list_payment_methods_by(kv), do: Repo.all(from pm in PaymentMethod, where: ^kv)

  @doc "Gets a single payment_method.Raises `Ecto.NoResultsError` if the Payment method does not exist."
  @spec get_payment_method!(number() | String.t()) :: PaymentMethod.t()
  def get_payment_method!(id), do: Repo.get!(PaymentMethod, id)

  @doc "get PaymentMethod by keyword"
  @spec get_payment_method_by(keyword()) :: PaymentMethod.t() | nil
  def get_payment_method_by(kv), do: Repo.get_by(PaymentMethod, kv)

  @doc "Creates a payment_method."
  @spec create_payment_method_with_stripe(map()) ::
          {:ok, PaymentMethod.t()} | {:error, Ecto.Changeset.t()}
  def create_payment_method_with_stripe(
        %{"customer_id" => cust_id, "stripe_id" => pm_id} = params
      ) do
    with {:ok, _resp} <- Billing.Stripe.attatch_payment_method(cust_id, pm_id) do
      create_payment_method(params)
    end
  end

  @doc "creates a PaymentMethod"
  @spec create_payment_method(map()) :: {:ok, PaymentMethod.t()} | {:error, Ecto.Changeset.t()}
  def create_payment_method(attrs \\ %{}) when is_map(attrs) do
    %PaymentMethod{}
    |> PaymentMethod.changeset(attrs)
    |> Repo.insert()
  end

  @doc "updates a PaymentMethod"
  @spec update_payment_method(PaymentMethod.t(), map()) ::
          {:ok, PaymentMethod.t()} | {:error, Ecto.Changeset.t()}
  def update_payment_method(%PaymentMethod{} = payment_method, attrs) do
    payment_method
    |> PaymentMethod.changeset(attrs)
    |> Repo.update()
  end

  @doc "Deletes a payment_method"
  @spec delete_payment_method(PaymentMethod.t()) ::
          {:ok, PaymentMethod.t()} | {:error, Ecto.Changeset.t()}
  def delete_payment_method(%PaymentMethod{} = payment_method) do
    Repo.delete(payment_method)
  end

  @doc "Deletes multiple payment_methods matching a keyword"
  @spec delete_all_payment_methods_by(keyword()) :: {non_neg_integer(), nil}
  def delete_all_payment_methods_by(kv), do: Repo.delete_all(from pm in PaymentMethod, where: ^kv)

  @doc "Deletes PaymentMethod both in db and stripe. User must have minimally 1 payment method"
  @spec delete_payment_method_with_stripe(PaymentMethod.t()) ::
          {:ok, PaymentMethod.t()} | {:error, String.t()}
  def delete_payment_method_with_stripe(%PaymentMethod{} = payment_method) do
    methods = list_payment_methods_by(customer_id: payment_method.customer_id)

    with count when count > 1 <- Enum.count(methods),
         {:ok, _response} <- Billing.Stripe.detach_payment_method(payment_method.stripe_id),
         {:ok, payment_method} <- delete_payment_method(payment_method) do
      {:ok, payment_method}
    else
      {:error, %Stripe.Error{message: message}} ->
        {:error, message}

      1 ->
        {:error, "You need at least one payment method!"}

      _err ->
        {:error, "Failed to delete payment method!"}
    end
  end

  @doc "Returns an `%Ecto.Changeset{}` for tracking payment_method changes."
  @spec change_payment_method(PaymentMethod.t()) :: Ecto.Changeset.t()
  def change_payment_method(%PaymentMethod{} = payment_method, attrs \\ %{}) do
    PaymentMethod.changeset(payment_method, attrs)
  end

  @doc "Syncs db PaymentMethod with Stripe as a source of truth."
  @spec sync_payment_methods(String.t()) ::
          {:ok, [PaymentMethod.t()]} | {:error, Stripe.Error.t()}
  def sync_payment_methods(cust_id) do
    with {:ok, %Stripe.List{data: stripe_payment_methods}} <-
           Billing.Stripe.list_payment_methods(cust_id),
         {_count, _} <- delete_all_payment_methods_by(customer_id: cust_id) do
      new_pms =
        for x <- stripe_payment_methods,
            {:ok, new_pm} =
              create_payment_method(%{
                stripe_id: x.id,
                customer_id: x.customer,
                last_four: x.card.last4,
                exp_month: x.card.exp_month,
                exp_year: x.card.exp_year,
                brand: x.card.brand
              }) do
          new_pm
        end

      {:ok, new_pms}
    end
  end

  # Plans

  @doc "Returns the list of plans."
  @spec list_plans() :: [Plan.t()]
  def list_plans do
    Repo.all(Plan)
  end

  @doc "Finds a plan from a list of plans"
  @spec find_plan([Plan.t()], String.t(), String.t()) :: Plan.t() | nil
  def find_plan(plans, period, name) do
    plans
    |> Enum.find(fn
      %{period: ^period, name: ^name} -> true
      _ -> false
    end)
  end

  @doc "Gets a single plan. Raises `Ecto.NoResultsError` if the Plan does not exist."
  @spec get_plan!(String.t() | number()) :: Plan.t()
  def get_plan!(id), do: Repo.get!(Plan, id)

  @doc "Gets a single plan by attribute. Returns Free plan if the plan does not exist."
  @spec get_plan_by(keyword()) :: Plan.t() | nil
  def get_plan_by(kw) do
    plan = Repo.get_by(Plan, kw)
    # nil plan blows everything up. Should never return a nil plan
    cond do
      is_nil(plan) and kw == [name: "Free"] ->
        raise "No Free Plan created yet in database."

      is_nil(plan) ->
        Logger.warning(
          "Customer is on a Stripe plan which doesn't exist in our plan list, defaulting to Free"
        )

        get_plan_by(name: "Free")

      true ->
        plan
    end
  end

  @doc """
  Retrieve a user's plan.

  Defaults to legacy plan if billing is not enabled.

  Returns Enterprise plan if single-tenant.

  """
  @spec get_plan_by_user(User.t()) :: Plan.t()
  def get_plan_by_user(%User{} = user) do
    cond do
      SingleTenant.single_tenant?() ->
        get_plan_by(name: "Enterprise")

      Partners.user_upgraded?(user) ->
        get_plan_by(name: "Enterprise")

      true ->
        billing_account = user.billing_enabled && get_billing_account_by(user_id: user.id)
        resolve_plan(user.billing_enabled, billing_account, &get_plan_by/1)
    end
  end

  @doc """
  Bulk-resolves plans for a list of users in a bounded number of queries.

  Equivalent to calling `get_plan_by_user/1` for each user, but batches the
  `partner_users` and `billing_accounts` lookups instead of issuing one query
  per user. Reuses an already-preloaded `:billing_account` association when
  present instead of re-querying it.
  """
  @spec get_plans_by_users([User.t()]) :: %{optional(pos_integer()) => Plan.t()}
  def get_plans_by_users(users) when is_list(users) do
    if SingleTenant.single_tenant?() do
      plan = get_plan_by(name: "Enterprise")
      Map.new(users, &{&1.id, plan})
    else
      plans = list_plans()
      plan_finder = &find_plan_in(plans, &1)
      upgraded_ids = fetch_upgraded_user_ids(users)
      billing_accounts_by_user_id = fetch_billing_accounts_by_user_id(users)

      Map.new(users, fn user ->
        billing_account = resolve_billing_account(user, billing_accounts_by_user_id)
        plan = resolve_user_plan(user, billing_account, upgraded_ids, plan_finder)
        {user.id, plan}
      end)
    end
  end

  defp upgraded?(%User{partner_upgraded: upgraded?}, _upgraded_ids) when is_boolean(upgraded?),
    do: upgraded?

  defp upgraded?(%User{id: id}, upgraded_ids), do: MapSet.member?(upgraded_ids, id)

  defp resolve_billing_account(user, billing_accounts_by_user_id) do
    if Ecto.assoc_loaded?(user.billing_account) do
      user.billing_account
    else
      Map.get(billing_accounts_by_user_id, user.id)
    end
  end

  defp resolve_user_plan(user, billing_account, upgraded_ids, plan_finder) do
    if upgraded?(user, upgraded_ids) do
      plan_finder.(name: "Enterprise")
    else
      resolve_plan(user.billing_enabled, billing_account, plan_finder)
    end
  end

  defp fetch_upgraded_user_ids(users) do
    case users |> Enum.reject(&is_boolean(&1.partner_upgraded)) |> Enum.map(& &1.id) do
      [] ->
        MapSet.new()

      pending_ids ->
        from(pu in "partner_users",
          where: pu.user_id in ^pending_ids and pu.upgraded == true,
          select: pu.user_id
        )
        |> Repo.all()
        |> MapSet.new()
    end
  end

  defp fetch_billing_accounts_by_user_id(users) do
    case users |> Enum.reject(&Ecto.assoc_loaded?(&1.billing_account)) |> Enum.map(& &1.id) do
      [] ->
        %{}

      missing_ids ->
        from(ba in BillingAccount, where: ba.user_id in ^missing_ids)
        |> Repo.all()
        |> Map.new(&{&1.user_id, &1})
    end
  end

  # Shared with `get_plan_by_user/1`: resolves the non-upgraded, non-single-tenant
  # branch of plan lookup. `plan_finder` abstracts over the underlying plan source
  # (a DB query for the single-user path, an in-memory list for the bulk path).
  defp resolve_plan(billing_enabled, billing_account, plan_finder) do
    cond do
      billing_enabled == false ->
        legacy_plan()

      billing_enabled ->
        case billing_account do
          nil ->
            plan_finder.(name: "Free")

          %BillingAccount{lifetime_plan: true} ->
            plan_finder.(name: "Lifetime")

          %BillingAccount{stripe_subscriptions: nil} ->
            plan_finder.(name: "Free")

          billing_account ->
            resolve_stripe_plan(billing_account, plan_finder)
        end
    end
  end

  defp resolve_stripe_plan(billing_account, plan_finder) do
    case get_billing_account_stripe_plan(billing_account) do
      nil ->
        plan_finder.(name: "Free")

      stripe_plan ->
        plan_finder.(stripe_id: stripe_plan["id"])
    end
  end

  defp find_plan_in(plans, name: "Free" = name) do
    Enum.find(plans, &(&1.name == name)) ||
      raise "No Free Plan created yet in database."
  end

  defp find_plan_in(plans, kw) do
    Enum.find(plans, fn plan -> Enum.all?(kw, fn {k, v} -> Map.get(plan, k) == v end) end) ||
      warn_and_fallback_to_free(plans, kw)
  end

  defp warn_and_fallback_to_free(plans, kw) do
    Logger.warning(
      "Customer is on a Stripe plan which doesn't exist in our plan list, defaulting to Free",
      plan_lookup: kw
    )

    find_plan_in(plans, name: "Free")
  end

  @doc "Creates a plan."
  @spec create_plan(map()) :: {:ok, Plan.t()} | {:error, Ecto.Changeset.t()}
  def create_plan(attrs \\ %{}) do
    %Plan{}
    |> Plan.changeset(attrs)
    |> Repo.insert()
  end

  @doc "Updates a plan."
  @spec update_plan(Plan.t(), map()) :: {:ok, Plan.t()} | {:error, Ecto.Changeset.t()}
  def update_plan(%Plan{} = plan, attrs) do
    plan
    |> Plan.changeset(attrs)
    |> Repo.update()
  end

  @doc "Deletes a plan."
  @spec delete_plan(Plan.t()) :: {:ok, Plan.t()} | {:error, Ecto.Changeset.t()}
  def delete_plan(%Plan{} = plan) do
    Repo.delete(plan)
  end

  @doc "Returns an `%Ecto.Changeset{}` for tracking plan changes."
  @spec change_plan(Plan.t()) :: Ecto.Changeset.t()
  def change_plan(%Plan{} = plan) do
    Plan.changeset(plan, %{})
  end

  @doc "Returns the legacy plan"
  @spec legacy_plan :: Plan.t()
  def legacy_plan do
    %Plan{
      limit_rate_limit: 150,
      limit_source_rate_limit: 50,
      name: "Legacy",
      limit_saved_search_limit: 1,
      limit_team_users_limit: 2,
      limit_source_fields_limit: 500
    }
  end

  @spec cost_estimate(Plan.t(), pos_integer()) :: pos_integer()
  def cost_estimate(%Plan{price: price}, usage), do: price * usage
end
