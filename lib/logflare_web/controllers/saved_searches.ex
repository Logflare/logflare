defmodule LogflareWeb.SavedSearchesController do
  use LogflareWeb, :controller

  alias Logflare.SavedSearches
  require Logger

  plug LogflareWeb.Plugs.SetVerifySource
       when action in [:delete]

  def delete(conn, %{"id" => search_id} = _params) do
    search_id
    |> SavedSearches.get()
    |> SavedSearches.delete_by_user()
    |> case do
      {:ok, _response} ->
        conn
        |> put_flash(:info, "Saved search deleted!")
        |> redirect(to: ~p"/dashboard")

      {:error, response} ->
        Logger.error("SavedSearchesController delete error: #{inspect(response)}",
          saved_search: %{id: search_id}
        )

        conn
        |> put_flash(:error, "Something went wrong!")
        |> redirect(to: ~p"/dashboard")
    end
  end
end
