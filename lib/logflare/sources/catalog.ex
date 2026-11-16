defmodule Logflare.Sources.Catalog do
  @moduledoc """
  Provides the lightweight source data required to expand user-authored queries.
  """

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User

  @spec list_by_user(User.t() | pos_integer()) :: [Source.t()]
  def list_by_user(%User{id: user_id}), do: list_by_user(user_id)

  def list_by_user(user_id) when is_integer(user_id) do
    Sources.list_source_catalog_by_user(user_id)
  end
end
