defmodule Logflare.SavedSearches do
  alias Logflare.{SavedSearch, Repo}

  def insert(querystring, source) do
    changeset =
      source
      |> Ecto.build_assoc(:saved_searches)
      |> SavedSearch.changeset(%{querystring: querystring})

    Repo.insert(changeset)
  end
end
