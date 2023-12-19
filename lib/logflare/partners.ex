defmodule Logflare.Partners do
  @moduledoc """
  Handles Partner context
  """
  import Ecto.Query

  alias Logflare.Partners.Partner
  alias Logflare.Partners.PartnerUser
  alias Logflare.Repo
  alias Logflare.Users
  alias Logflare.User
  alias Ecto.Multi
  @spec get_partner(integer()) :: Partner.t() | nil
  @doc """
  Fetch single partner by given id
  """
  def get_partner(id), do: Repo.get(Partner, id)

  @spec create_partner(binary()) :: {:ok, Partner.t()} | {:error, any()}
  @doc """
  Creates a new partner with given name and token to be encrypted
  """
  def create_partner(name) do
    %Partner{}
    |> Partner.changeset(%{name: name})
    |> Repo.insert()
  end

  @spec list_partners() :: [Partner.t()]
  @doc """
  Lists all partners
  """
  def list_partners(), do: Repo.all(Partner)

  @spec get_partner_by_token(binary()) :: Partner.t() | nil
  @doc """
  Fetch single entry by given token
  """
  def get_partner_by_token(token), do: Repo.get_by(Partner, token: token)

  @spec create_user(Partner.t(), map()) ::
          {:ok, User.t()} | {:error, any()}
  @doc """
  Creates a new user and associates it with given partner
  """
  def create_user(%Partner{} = partner, params) do
    params = Map.merge(params, %{"provider" => "email"})

    Repo.transaction(fn ->
      with {:ok, user} <- Users.insert_user(params),
           {:ok, _} <- associate_user_to_partner(partner, user) do
        user
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
  end

  defp associate_user_to_partner(%Partner{id: partner_id}, %User{id: user_id}) do
    entry = PartnerUser.changeset(%PartnerUser{}, %{partner_id: partner_id, user_id: user_id})
    Repo.insert(entry)
  end

  @spec delete_partner_by_token(binary()) :: {:ok, Partner.t()} | {:error, any()}
  @doc """
  Deletes a partner given his token
  """
  def delete_partner_by_token(token) do
    token
    |> get_partner_by_token()
    |> Repo.delete()
  end

  @spec get_user_by_token(Partner.t(), binary()) :: User.t() | nil

  @doc """
  Fetches user by token for a given Partner
  """
  def get_user_by_token(%Partner{token: token}, user_token) do
    query =
      from(p in Partner,
        join: u in assoc(p, :users),
        where: p.token == ^token,
        where: u.token == ^user_token,
        select: u
      )

    Repo.one(query)
  end

  @doc """
  Deletes user if user was created by given partner
  """
  @spec delete_user(Partner.t(), User.t()) :: {:ok, User.t()} | {:error, any()}
  def delete_user(%Partner{} = partner, %User{token: user_token}) do
    Repo.transaction(fn ->
      with user when not is_nil(user) <- get_user_by_token(partner, user_token),
           {:ok, _} <- diassociate_user_from_partner(partner, user),
           {:ok, _} <- Users.delete_user(user) do
        user
      else
        nil -> Repo.rollback(:not_found)
        {:error, error} -> Repo.rollback(error)
      end
    end)
  end

  defp diassociate_user_from_partner(%Partner{id: partner_id}, %User{id: user_id}) do
    query =
      from(pu in PartnerUser,
        where: pu.partner_id == ^partner_id,
        where: pu.user_id == ^user_id,
        select: pu.id
      )

    Multi.new()
    |> Multi.delete_all(:delete, query)
    |> Repo.transaction()
  end

  def user_upgraded?(%User{id: id}) do
    query =
      from(pu in PartnerUser, where: pu.user_id == ^id, select: pu.upgraded)

    Repo.one(query) || false
  end

  def upgrade_user(p, u), do: do_upgrade_downgrade(p, u, true)
  def downgrade_user(p, u), do: do_upgrade_downgrade(p, u, false)

  def do_upgrade_downgrade(%Partner{id: partner_id}, %User{id: user_id}, value) do
    query =
      from(pu in PartnerUser,
        where: pu.partner_id == ^partner_id and pu.user_id == ^user_id,
        select: pu
      )

    case Repo.update_all(query, set: [upgraded: value]) do
      {1, [partner_user]} -> {:ok, partner_user}
      _ -> {:error, :not_found}
    end
  end
end
