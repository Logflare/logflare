defmodule Logflare.Vercel do
  alias Logflare.Vercel.Auth
  alias Logflare.Repo

  def find_by_or_create_auth(kv, user, attrs \\ %{}) do
    case get_auth_by(kv) do
      nil -> create_auth(user, attrs)
      auth -> {:ok, auth}
    end
  end

  @doc """
  Returns the list of vercel_auths.

  ## Examples

      iex> list_vercel_auths()
      [%Auth{}, ...]

  """
  def list_vercel_auths do
    Repo.all(Auth)
  end

  @doc """
  Gets a single auth.

  Raises `Ecto.NoResultsError` if the Auth does not exist.

  ## Examples

      iex> get_auth!(123)
      %Auth{}

      iex> get_auth!(456)
      ** (Ecto.NoResultsError)

  """
  def get_auth!(id), do: Repo.get!(Auth, id)

  def get_auth_by(kv), do: Repo.get_by(Auth, kv)

  @doc """
  Creates a auth.

  ## Examples

      iex> create_auth(%{field: value})
      {:ok, %Auth{}}

      iex> create_auth(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_auth(user, attrs \\ %{}) do
    user
    |> Ecto.build_assoc(:vercel_auths)
    |> Auth.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a auth.

  ## Examples

      iex> update_auth(auth, %{field: new_value})
      {:ok, %Auth{}}

      iex> update_auth(auth, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_auth(%Auth{} = auth, attrs) do
    auth
    |> Auth.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a auth.

  ## Examples

      iex> delete_auth(auth)
      {:ok, %Auth{}}

      iex> delete_auth(auth)
      {:error, %Ecto.Changeset{}}

  """
  def delete_auth(%Auth{} = auth) do
    Repo.delete(auth)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking auth changes.

  ## Examples

      iex> change_auth(auth)
      %Ecto.Changeset{data: %Auth{}}

  """
  def change_auth(%Auth{} = auth, attrs \\ %{}) do
    Auth.changeset(auth, attrs)
  end
end
