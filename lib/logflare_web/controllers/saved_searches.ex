defmodule LogflareWeb.SavedSearchesController do
  use LogflareWeb, :controller
  alias Logflare.{SavedSearches, Sources}

  plug LogflareWeb.Plugs.SetVerifySource
       when action in [:delete]

  def delete(conn, %{"id" => search_id} = _params) do
    source = conn.assigns.source |> Sources.preload_saved_searches()

    saved_search =
      Enum.find(source.saved_searches, fn x ->
        x.id == String.to_integer(search_id)
      end)

    case SavedSearches.delete(saved_search) do
      {:ok, _response} ->
        conn
        |> put_flash(:info, "Saved search deleted!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      {:error, _response} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
  end
end
