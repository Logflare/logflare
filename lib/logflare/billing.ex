defmodule Logflare.Billing do
  @moduledoc false
  require Logger
  alias __MODULE__
  import Ecto.Query, warn: false
  alias Logflare.{Repo, Users, Source, User, Billing.Plan, Billing.BillingAccount, Billing.PaymentMethod}
  require Protocol
  Protocol.derive(Jason.Encoder, Stripe.List)
  Protocol.derive(Jason.Encoder, Stripe.Subscription)
  Protocol.derive(Jason.Encoder, Stripe.Plan)
  Protocol.derive(Jason.Encoder, Stripe.SubscriptionItem)
  Protocol.derive(Jason.Encoder, Stripe.Session)
  Protocol.derive(Jason.Encoder, Stripe.Invoice)
  Protocol.derive(Jason.Encoder, Stripe.LineItem)
  Protocol.derive(Jason.Encoder, Stripe.Price)
  Protocol.derive(Jason.Encoder, Stripe.Discount)
  Protocol.derive(Jason.Encoder, Stripe.Coupon)

  # BillingAccount

  @doc "Returns the list of billing_accounts"
  @spec list_billing_accounts() :: [%BillingAccount{}]
  def list_billing_accounts, do: Repo.all(BillingAccount)

  @doc "Gets a single billing_account by a keyword."
  @spec get_billing_account_by(keyword()) :: %BillingAccount{} | nil
  def get_billing_account_by(kv), do: Repo.get_by(BillingAccount, kv)

  @doc "Gets a single billing_account. Raises `Ecto.NoResultsError` if the Billing account does not exist."
  @spec get_billing_account!(String.t() | number()) :: %BillingAccount{}
  def get_billing_account!(id), do: Repo.get!(BillingAccount, id)

  @doc "Creates a billing_account."
  @spec create_billing_account(%User{}, map()) ::
          {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
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

      {:error, _changeset = res} ->
        res
    end
  end

  @doc "Syncs stripe subscription with %BillingAccount{} with Stripe as source of truth."
  @spec sync_subscriptions(nil | %BillingAccount{}) ::
          :noop | {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
  def sync_subscriptions(nil), do: :noop

  def sync_subscriptions(%BillingAccount{stripe_customer: stripe_customer_id} = billing_account) do
    with {:ok, subscriptions} <-
           Billing.Stripe.list_customer_subscriptions(stripe_customer_id) do
      update_billing_account(billing_account, %{stripe_subscriptions: subscriptions})
    end
  end

  @doc "Syncs stripe invoices with %BillingAccount{} with Stripe as source of truth."
  @spec sync_invoices(nil | %BillingAccount{}) ::
          :noop | {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
  def sync_invoices(nil), do: :noop

  def sync_invoices(%BillingAccount{stripe_customer: stripe_customer_id} = billing_account) do
    with {:ok, invoices} <- Billing.Stripe.list_customer_invoices(stripe_customer_id) do
      attrs = %{stripe_invoices: invoices}

      update_billing_account(billing_account, attrs)
    else
      err -> err
    end
  end

  @doc "Syncs stripe data with %BillingAccount{} with Stripe as source of truth."
  @spec sync_billing_account(nil | %BillingAccount{}) ::
          :noop | {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
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
  @spec update_billing_account(%BillingAccount{}, map()) ::
          {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
  def update_billing_account(%BillingAccount{} = billing_account, attrs) do
    billing_account
    |> BillingAccount.changeset(attrs)
    |> Repo.update()
  end

  @doc "Preloads the payment methods field"
  @spec preload_payment_methods(%BillingAccount{}) :: %BillingAccount{}
  def preload_payment_methods(ba), do: Repo.preload(ba, :payment_methods)

  @doc "Deletes a BillingAccount for a User"
  @spec delete_billing_account(%User{}) :: {:ok, %BillingAccount{}} | {:error, %Ecto.Changeset{}}
  def delete_billing_account(%User{billing_account: billing_account} = user) do
    with {:ok, _} = res <- Repo.delete(billing_account) do
      Source.Supervisor.reset_all_user_sources(user)
      res
    end
  end

  @doc "Returns an `%Ecto.Changeset{}` for tracking billing_account changes."
  @spec change_billing_account(%BillingAccount{}) :: %Ecto.Changeset{}
  def change_billing_account(%BillingAccount{} = billing_account) do
    BillingAccount.changeset(billing_account, %{})
  end

  @doc "retrieves the stripe plan stored on the BillingAccount"
  @spec get_billing_account_stripe_plan(%BillingAccount{}) :: nil | term()
  def get_billing_account_stripe_plan(%BillingAccount{
        stripe_subscriptions: %{"data" => [%{"plan" => plan} | _]}
      }),
      do: plan

  def get_billing_account_stripe_plan(_), do: nil

  @doc "gets the stripe subscription item data stored on the BillingAccount"
  @spec get_billing_account_stripe_subscription_item(%BillingAccount{}) :: nil | term()

  def get_billing_account_stripe_subscription_item(%BillingAccount{
        stripe_subscriptions: %{
          "data" => [%{"items" => [%{"data" => [item | _]} | _]} | _]
        }
      }),
      do: item

  def get_billing_account_stripe_subscription_item(_), do: nil

  # PaymentMethod

  @doc "list PaymentMethod by keyword"
  @spec list_payment_methods_by(keyword()) :: [%PaymentMethod{}]
  def list_payment_methods_by(kv), do: Repo.all(from pm in PaymentMethod, where: ^kv)

  @doc "Gets a single payment_method.Raises `Ecto.NoResultsError` if the Payment method does not exist."
  @spec get_payment_method!(number() | String.t()) :: %PaymentMethod{}
  def get_payment_method!(id), do: Repo.get!(PaymentMethod, id)

  @doc "get PaymentMethod by keyword"
  @spec get_payment_method_by(keyword()) :: %PaymentMethod{}
  def get_payment_method_by(kv), do: Repo.get_by(PaymentMethod, kv)

  @doc "Creates a payment_method."
  @spec create_payment_method_with_stripe(map()) ::
          {:ok, %PaymentMethod{}} | {:error, %Ecto.Changeset{}}
  def create_payment_method_with_stripe(
        %{"customer_id" => cust_id, "stripe_id" => pm_id} = params
      ) do
    with {:ok, _resp} <- Billing.Stripe.attatch_payment_method(cust_id, pm_id) do
      create_payment_method(params)
    end
  end

  @doc "creates a PaymentMethod"
  @spec create_payment_method(map()) :: {:ok, %PaymentMethod{}} | {:error, %Ecto.Changeset{}}
  def create_payment_method(attrs \\ %{}) when is_map(attrs) do
    %PaymentMethod{}
    |> PaymentMethod.changeset(attrs)
    |> Repo.insert()
  end

  @doc "updates a PaymentMethod"
  @spec update_payment_method(%PaymentMethod{}, map()) ::
          {:ok, %PaymentMethod{}} | {:error, %Ecto.Changeset{}}
  def update_payment_method(%PaymentMethod{} = payment_method, attrs) do
    payment_method
    |> PaymentMethod.changeset(attrs)
    |> Repo.update()
  end

  @doc "Deletes a payment_method"
  @spec delete_payment_method(%PaymentMethod{}) ::
          {:ok, %PaymentMethod{}} | {:error, %Ecto.Changeset{}}
  def delete_payment_method(%PaymentMethod{} = payment_method) do
    Repo.delete(payment_method)
  end

  @doc "Deletes multiple payment_methods matching a keyword"
  @spec delete_all_payment_methods_by(keyword()) :: {integer(), nil}
  def delete_all_payment_methods_by(kv), do: Repo.delete_all(from pm in PaymentMethod, where: ^kv)

  @doc "Deletes PaymentMethod both in db and stripe. User must have minimally 1 payment method"
  @spec delete_payment_method_with_stripe(%PaymentMethod{}) ::
          {:ok, %PaymentMethod{}} | {:error, String.t()}
  def delete_payment_method_with_stripe(%PaymentMethod{} = payment_method) do
    with methods <- list_payment_methods_by(customer_id: payment_method.customer_id),
         count when count > 1 <- Enum.count(methods),
         {:ok, _respons} <- Billing.Stripe.detach_payment_method(payment_method.stripe_id) do
      delete_payment_method(payment_method)
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
  @spec change_payment_method(%PaymentMethod{}) :: %Ecto.Changeset{}
  def change_payment_method(%PaymentMethod{} = payment_method, attrs \\ %{}) do
    PaymentMethod.changeset(payment_method, attrs)
  end

  @doc "Syncs db PaymentMethod with stripe as a souce of truth"
  @spec sync_payment_methods(String.t()) :: {:ok, [%PaymentMethod{}]}
  def sync_payment_methods(cust_id) do
    with {:ok, %Stripe.List{data: stripe_payment_methods}} <-
           Billing.Stripe.list_payment_methods(cust_id),
         {_count, _} <- delete_all_payment_methods_by(customer_id: cust_id) do
      new_pms =
        for x <- stripe_payment_methods,
            {:ok, new_pm} <-
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
  def list_plans do
    Repo.all(Plan)
  end

  def find_plan(plans, period, name) do
    Enum.filter(plans, fn x -> x.period == period end)
    |> Enum.find(fn x -> x.name == name end)
  end

  @doc """
  Gets a single plan.

  Raises `Ecto.NoResultsError` if the Plan does not exist.

  ## Examples

      iex> get_plan!(123)
      %Plan{}

      iex> get_plan!(456)
      ** (Ecto.NoResultsError)

  """
  def get_plan!(id), do: Repo.get!(Plan, id)

  def get_plan_by(kw) do
    plan = Repo.get_by(Plan, kw)

    # nil plan blows everything up. Should never return a nil plan.

    if is_nil(plan) do
      Logger.error("Customer is on a Stripe plan which doesn't exist in our plan list!!!!!")

      get_plan_by(name: "Free")
    else
      plan
    end
  end

  def get_plan_by_user(%User{} = user) do
    if user.billing_enabled do
      case Billing.get_billing_account_by(user_id: user.id) do
        nil ->
          get_plan_by(name: "Free")

        %Billing.BillingAccount{lifetime_plan: true} ->
          get_plan_by(name: "Lifetime")

        %Billing.BillingAccount{stripe_subscriptions: nil} ->
          get_plan_by(name: "Free")

        billing_account ->
          case Billing.get_billing_account_stripe_plan(billing_account) do
            nil ->
              get_plan_by(name: "Free")

            stripe_plan ->
              get_plan_by(stripe_id: stripe_plan["id"])
          end
      end
    else
      legacy_plan()
    end
  end

  @doc """
  Creates a plan.

  ## Examples

      iex> create_plan(%{field: value})
      {:ok, %Plan{}}

      iex> create_plan(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_plan(attrs \\ %{}) do
    %Plan{}
    |> Plan.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a plan.

  ## Examples

      iex> update_plan(plan, %{field: new_value})
      {:ok, %Plan{}}

      iex> update_plan(plan, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_plan(%Plan{} = plan, attrs) do
    plan
    |> Plan.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a plan.

  ## Examples

      iex> delete_plan(plan)
      {:ok, %Plan{}}

      iex> delete_plan(plan)
      {:error, %Ecto.Changeset{}}

  """
  def delete_plan(%Plan{} = plan) do
    Repo.delete(plan)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking plan changes.

  ## Examples

      iex> change_plan(plan)
      %Ecto.Changeset{source: %Plan{}}

  """
  def change_plan(%Plan{} = plan) do
    Plan.changeset(plan, %{})
  end

  def legacy_plan() do
    %Plan{
      limit_rate_limit: 150,
      limit_source_rate_limit: 50,
      name: "Legacy",
      limit_saved_search_limit: 1,
      limit_team_users_limit: 2,
      limit_source_fields_limit: 500
    }
  end


end
