defmodule LogflareWeb.Api.FallbackController do
  use Phoenix.Controller
  alias Ecto.Changeset

  def call(conn, {:error, %Changeset{} = changeset}) do
    errors = Changeset.traverse_errors(changeset, fn _, _, {message, _} -> "#{message}" end)

    conn
    |> put_status(:unprocessable_entity)
    |> json(%{errors: errors})
  end
end
