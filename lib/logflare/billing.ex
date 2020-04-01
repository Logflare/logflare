defmodule Logflare.Billing do
  @moduledoc """
  The Billing context.
  """

  import Ecto.Query, warn: false
  alias Logflare.Repo
  alias Logflare.Users
  alias Logflare.Billing.BillingAccount

  @doc """
  Returns the list of billing_accounts.

  ## Examples

      iex> list_billing_accounts()
      [%BillingAccount{}, ...]

  """
  def list_billing_accounts do
    Repo.all(BillingAccount)
  end

  @doc """
  Gets a single billing_account by a keyword.
  """
  def get_billing_account_by(kv) do
    Repo.get_by(BillingAccount, kv)
  end

  @doc """
  Gets a single billing_account.

  Raises `Ecto.NoResultsError` if the Billing account does not exist.

  ## Examples

      iex> get_billing_account!(123)
      %BillingAccount{}

      iex> get_billing_account!(456)
      ** (Ecto.NoResultsError)

  """
  def get_billing_account!(id), do: Repo.get!(BillingAccount, id)

  @doc """
  Creates a billing_account.

  ## Examples

      iex> create_billing_account(%{field: value})
      {:ok, %BillingAccount{}}

      iex> create_billing_account(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_billing_account(user_id, attrs \\ %{}) do
    Users.get(user_id)
    |> Ecto.build_assoc(:billing_account)
    |> BillingAccount.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a billing_account.

  ## Examples

      iex> update_billing_account(billing_account, %{field: new_value})
      {:ok, %BillingAccount{}}

      iex> update_billing_account(billing_account, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_billing_account(%BillingAccount{} = billing_account, attrs) do
    billing_account
    |> BillingAccount.changeset(attrs)
    |> Repo.update()
  end

  def create_or_update_billing_account(user_id, attrs) do
    case get_billing_account_by(user_id: user_id) do
      nil ->
        {:ok, billing_account} = create_billing_account(user_id, attrs)
        {:ok, :created, billing_account}

      billing_account ->
        {:ok, billing_account} = update_billing_account(billing_account, attrs)
        {:ok, :updated, billing_account}
    end
  end

  @doc """
  Deletes a BillingAccount.

  ## Examples

      iex> delete_billing_account(billing_account)
      {:ok, %BillingAccount{}}

      iex> delete_billing_account(billing_account)
      {:error, %Ecto.Changeset{}}

  """
  def delete_billing_account(%BillingAccount{} = billing_account) do
    Repo.delete(billing_account)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking billing_account changes.

  ## Examples

      iex> change_billing_account(billing_account)
      %Ecto.Changeset{source: %BillingAccount{}}

  """
  def change_billing_account(%BillingAccount{} = billing_account) do
    BillingAccount.changeset(billing_account, %{})
  end
end
