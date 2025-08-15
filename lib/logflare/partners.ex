defmodule Logflare.Partners do
  @moduledoc """
  Handles Partner context
  """
  import Ecto.Query

  alias Logflare.Partners.Partner
  alias Logflare.Repo
  alias Logflare.Users
  alias Logflare.User
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
  def list_partners, do: Repo.all(Partner)

  @spec get_partner_by_uuid(binary()) :: Partner.t() | nil
  @doc """
  Fetch single entry by given uuid based on the :token field
  """
  def get_partner_by_uuid(uuid), do: Repo.get_by(Partner, token: uuid)

  @spec create_user(Partner.t(), map()) ::
          {:ok, User.t()} | {:error, any()}
  @doc """
  Creates a new user and associates it with given partner
  """
  def create_user(%Partner{} = partner, params) do
    params = Map.merge(params, %{"provider" => "email", "partner_id" => partner.id})

    Users.insert_user(params)
  end

  @spec delete_partner_by_token(binary()) :: {:ok, Partner.t()} | {:error, any()}
  @doc """
  Deletes a partner given his token
  """
  def delete_partner_by_token(token) do
    token
    |> get_partner_by_uuid()
    |> Repo.delete()
  end

  @spec get_user_by_uuid(Partner.t(), binary()) :: User.t() | nil

  @doc """
  Fetches user by uuid (token field) for a given Partner
  """
  def get_user_by_uuid(%Partner{id: id}, user_uuid) do
    query =
      from(u in User,
        where: u.partner_id == ^id and u.token == ^user_uuid
      )

    Repo.one(query)
  end

  @doc """
  Deletes user if user was created by given partner
  """
  @spec delete_user(Partner.t(), User.t()) :: {:ok, User.t()} | {:error, any()}
  def delete_user(%Partner{id: partner_id}, %User{partner_id: user_partner_id} = user)
      when user_partner_id == partner_id do
    Users.delete_user(user)
  end

  def delete_user(%Partner{id: partner_id}, %User{partner_id: user_partner_id})
      when user_partner_id != partner_id do
    {:error, :not_found}
  end

  def user_upgraded?(%User{partner_upgraded: value}) when is_boolean(value), do: value

  def user_upgraded?(%{id: user_id}) do
    query = from(u in "partner_users", where: u.user_id == ^user_id, select: u.upgraded, limit: 1)
    Repo.one(query) || false
  end

  def user_upgraded?(_), do: false

  def upgrade_user(u), do: do_upgrade_downgrade(u, true)
  def downgrade_user(u), do: do_upgrade_downgrade(u, false)

  def do_upgrade_downgrade(%User{partner_id: nil}, _value) do
    {:error, :no_partner}
  end

  def do_upgrade_downgrade(%User{partner_id: partner_id} = user, value)
      when is_boolean(value) and partner_id != nil do
    # backwards compat
    Repo.update_all(
      from(u in "partner_users", where: u.user_id == ^user.id and u.partner_id == ^partner_id),
      set: [upgraded: value]
    )

    Users.update_user_all_fields(user, %{partner_upgraded: value})
  end
end
