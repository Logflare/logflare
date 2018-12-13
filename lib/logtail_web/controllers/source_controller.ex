defmodule LogtailWeb.SourceController do
  use LogtailWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug LogtailWeb.Plugs.RequireAuth when action in [:new, :create, :edit, :update, :delete, :dashboard]

  alias Logtail.User
  alias Logtail.Source
  alias Logtail.Repo


  def index(conn, _params) do
    render conn, "index.html"
  end

  def dashboard(conn, _params) do
    user_id = conn.assigns.user.id
    query = from s in "sources",
          where: s.user_id == ^user_id,
          select: s.name
    sources = Repo.all(query)

    render conn, "dashboard.html", sources: sources
  end

  def new(conn, _params) do
    changeset = Source.changeset(%Source{}, %{})

    render conn, "new.html", changeset: changeset
  end

  def create(conn, %{"source" => source}) do
    changeset = conn.assigns.user
      |> Ecto.build_assoc(:sources)
      |> Source.changeset(source)
    IO.inspect(changeset)

    case Repo.insert(changeset) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Source created!")
        |> redirect(to: source_path(conn, :dashboard))
      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render "new.html", changeset: changeset
    end
 end

end
