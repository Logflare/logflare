defmodule Logflare.SingleTenant do
  @moduledoc """
  Handles single tenant-related logic
  """
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.Billing.Plan

  @user_attrs %{
    name: "default",
    email: "default@logflare.app",
    email_preferred: "default@logflare.app",
    provider: "default",
    token: Ecto.UUID.generate(),
    provider_uid: "default",
    endpoints_beta: true
  }
  @plan_attrs %{
    name: "Enterprise",
    period: "year",
    price: 20_000,
    limit_sources: 100,
    limit_rate_limit: 5_000,
    limit_alert_freq: 1_000,
    limit_source_rate_limit: 100,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    type: "standard"
  }

  @doc """
  Retrieves the default user
  """
  def get_default_user do
    Users.get_by(provider_uid: "default", provider: "default")
  end

  @doc """
  Retrieves the default plan
  """
  def get_default_plan do
    @plan_attrs
    |> Map.to_list()
    |> Billing.get_plan_by()
  end

  @doc """
  Creates an enterprise user
  """
  def create_default_user do
    case Users.insert_user(@user_attrs) do
      {:ok, _} = result ->
        result

      {:error, %Ecto.Changeset{errors: [email: {_, [{:constraint, :unique} | _]}]}} ->
        {:error, :already_created}
    end
  end

  @doc """
  Creates a default enterprise plan for single tenant
  """
  @spec create_default_plan() :: {:ok, Plan.t()} | {:error, :already_created}
  def create_default_plan do
    plan =
      Billing.list_plans()
      |> Enum.find(fn plan -> plan.name == "Enterprise" end)

    if plan == nil do
      Billing.create_plan(@plan_attrs)
    else
      {:error, :already_created}
    end
  end

  def single_tenant? do
    if Application.get_env(:logflare, :single_tenant) do
      true
    else
      false
    end
  end
end
