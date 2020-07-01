defmodule Logflare.Plans do
  @moduledoc """
  The Plans context.
  """

  import Ecto.Query, warn: false
  alias Logflare.Repo
  alias Logflare.BillingAccounts
  alias Logflare.Plans.Plan
  alias Logflare.User

  @doc """
  Returns the list of plans.

  ## Examples

      iex> list_plans()
      [%Plan{}, ...]

  """
  def list_plans do
    Repo.all(Plan)
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

  def get_plan_by(kw), do: Repo.get_by(Plan, kw)

  def get_plan_by_user(%User{} = user) do
    if user.billing_enabled? do
      case BillingAccounts.get_billing_account_by(user_id: user.id) do
        nil ->
          get_plan_by(name: "Free")

        %BillingAccounts.BillingAccount{stripe_subscriptions: nil} ->
          get_plan_by(name: "Free")

        billing_account ->
          {:ok, stripe_plan} = BillingAccounts.get_billing_account_stripe_plan(billing_account)

          get_plan_by(stripe_id: stripe_plan["id"])
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
    %Plan{limit_rate_limit: 150, limit_source_rate_limit: 50, name: "Legacy"}
  end
end
