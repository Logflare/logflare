defmodule LogflareWeb.Api.FallbackController do
  use Phoenix.Controller
  alias Ecto.Changeset

  def call(conn, {:error, %Changeset{} = changeset}) do
    errors = Changeset.traverse_errors(changeset, fn _, _, {message, _} -> message end)

    conn
    |> put_status(:unprocessable_entity)
    |> json(%{errors: errors})
  end

  def call(conn, {:error, :unauthorized}) do
    conn
    |> put_status(401)
    |> json(%{error: "Unauthorized"})
    |> halt()
  end

  def call(conn, {:error, :buffer_full}) do
    conn
    |> put_status(429)
    |> json(%{error: "Buffer Full: Too Many Requests"})
    |> halt()
  end

  def call(conn, {:error, :not_found}) do
    conn
    |> put_status(:not_found)
    |> json(%{error: "Not Found"})
  end

  def call(conn, {:error, %{} = err_map}) do
    conn
    |> put_status(400)
    |> json(%{error: err_map})
  end

  def call(conn, {:error, msg}) when is_atom(msg) do
    call(conn, {:error, Atom.to_string(msg)})
  end

  def call(conn, {:error, msg}) when is_binary(msg) do
    conn
    |> put_status(400)
    |> json(%{error: msg})
  end
end
