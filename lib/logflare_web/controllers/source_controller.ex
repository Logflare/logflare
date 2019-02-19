defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug(LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create])

  alias Logflare.Source
  alias Logflare.Repo
  alias LogflareWeb.AuthController

  def index(conn, _params) do
    render(conn, "index.html")
  end

  def dashboard(conn, _params) do
    user_id = conn.assigns.user.id

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token
        }
      )

    sources =
      for source <- Repo.all(query) do
        log_count = get_log_count(source)
        Map.put(source, :log_count, log_count)
      end

    render(conn, "dashboard.html", sources: sources)
  end

  def new(conn, _params) do
    changeset = Source.changeset(%Source{}, %{})
    render(conn, "new.html", changeset: changeset)
  end

  def create(conn, %{"source" => source}) do
    user = conn.assigns.user

    changeset =
      user
      |> Ecto.build_assoc(:sources)
      |> Source.changeset(source)

    oauth_params = get_session(conn, :oauth_params)

    case Repo.insert(changeset) do
      {:ok, _source} ->
        case is_nil(oauth_params) do
          true ->
            conn
            |> put_flash(:info, "Source created!")
            |> redirect(to: Routes.source_path(conn, :dashboard))

          false ->
            conn
            |> put_flash(:info, "Source created!")
            |> AuthController.redirect_for_oauth(user)
        end

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("new.html", changeset: changeset)
    end
  end

  def show(conn, %{"id" => source_id}) do
    user_id = conn.assigns.user.id
    source = Repo.get(Source, source_id)

    case user_id == source.user_id do
      true ->
        IO.inspect(source)
        table_id = String.to_atom(source.token)
        logs = get_logs(table_id)
        render(conn, "show.html", logs: logs, source: source)

      false ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.source_path(conn, :index))
        |> halt()
    end
  end

  def public(conn, %{"public_token" => public_token}) do
    source = Repo.get_by(Source, public_token: public_token)

    case source == nil do
      true ->
        conn
        |> put_flash(:error, "Public path not found!")
        |> redirect(to: Routes.source_path(conn, :index))

      false ->
        table_id = String.to_atom(source.token)
        logs = get_logs(table_id)
        render(conn, "show.html", logs: logs, source: source)
    end
  end

  def edit(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    user_id = conn.assigns.user.id

    case user_id == source.user_id do
      true ->
        changeset = Source.changeset(source, %{})

        query =
          from(s in "sources",
            where: s.user_id == ^user_id,
            select: %{
              name: s.name,
              id: s.id,
              token: s.token,
              public_token: s.public_token
            }
          )

        sources =
          for source <- Repo.all(query) do
            {:ok, token} = Ecto.UUID.load(source.token)
            Map.put(source, :token, token)
          end

        render(conn, "edit.html", changeset: changeset, source: source, sources: sources)

      false ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.source_path(conn, :index))
        |> halt()
    end
  end

  def update(conn, %{"id" => source_id, "source" => source}) do
    user_id = conn.assigns.user.id
    old_source = Repo.get(Source, source_id)
    changeset = Source.changeset(old_source, source)

    case user_id == old_source.user_id do
      true ->
        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Source updated!")
            |> redirect(to: Routes.source_path(conn, :edit, source_id))

          {:error, changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> render("edit.html", changeset: changeset, source: old_source)
        end

      false ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.source_path(conn, :index))
        |> halt()
    end
  end

  def delete(conn, %{"id" => source_id}) do
    user_id = conn.assigns.user.id
    source = Repo.get(Source, source_id)

    case user_id == source.user_id do
      true ->
        source |> Repo.delete!()

        conn
        |> put_flash(:info, "Source deleted!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      false ->
        conn
        |> put_flash(:error, "That's not yours!")
        |> redirect(to: Routes.source_path(conn, :index))
        |> halt()
    end
  end

  defp get_logs(table_id) do
    case :ets.info(table_id) do
      :undefined ->
        []

      _ ->
        List.flatten(:ets.match(table_id, {:_, :"$1"}))
    end
  end

  defp get_log_count(source) do
    log_table_info = :ets.info(String.to_atom(elem(Ecto.UUID.load(source.token), 1)))

    case log_table_info do
      :undefined ->
        0

      _ ->
        log_table_info[:size]
    end
  end
end
