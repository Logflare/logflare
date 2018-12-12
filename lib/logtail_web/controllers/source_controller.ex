defmodule LogtailWeb.SourceController do
  use LogtailWeb, :controller

  alias Logtail.User
  alias Logtail.Repo
  alias Logtail.Source

  def index(conn, _params) do
    render conn, "index.html"
  end

  def new(conn, _params) do
    changeset = Source.changeset(%Source{}, %{})

    render conn, "new.html", changeset: changeset
  end

#  def create(conn, %{"source" => source}) do
#    changeset = conn.assigns.user
#      |> build_assoc(:sources)
#      |> Source.changeset(source)
#
#    case Repo.insert(changeset) do
#      {:ok, _topic} ->
#        conn
#        |> put_flash(:info, "Source created!")
#        |> redirect(to: source_path(conn, :index))
#      {:error, changeset} ->
#        render conn, "new.html", changeset: changeset
#    end
#  end

end
