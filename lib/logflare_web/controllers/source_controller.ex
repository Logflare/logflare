defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug(LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create])

  plug(
    LogflareWeb.Plugs.VerifySourceOwner
    when action in [:show, :edit, :update, :delete, :clear_logs, :favorite]
  )

  alias Logflare.Source
  alias Logflare.Repo
  alias LogflareWeb.AuthController
  alias Logflare.SystemCounter
  alias Logflare.SourceData
  alias Logflare.TableManager

  @system_counter :total_logs_logged

  def index(conn, _params) do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    render(conn, "index.html", log_count: log_count)
  end

  def dashboard(conn, _params) do
    user_id = conn.assigns.user.id

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: [desc: s.favorite],
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          favorite: s.favorite
        }
      )

    sources =
      for source <- Repo.all(query) do
        log_count = SourceData.get_log_count(source)
        rate = SourceData.get_rate(source)
        {:ok, token} = Ecto.UUID.load(source.token)
        timestamp = SourceData.get_latest_date(source)
        average_rate = SourceData.get_avg_rate(source)
        max_rate = SourceData.get_max_rate(source)

        Map.put(source, :log_count, log_count)
        |> Map.put(:rate, rate)
        |> Map.put(:token, token)
        |> Map.put(:latest, timestamp)
        |> Map.put(:avg, average_rate)
        |> Map.put(:max, max_rate)
      end

    render(conn, "dashboard.html", sources: sources)
  end

  def favorite(conn, %{"id" => source_id}) do
    old_source = Repo.get(Source, source_id)
    source = %{"favorite" => !old_source.favorite}
    changeset = Source.changeset(old_source, source)

    case Repo.update(changeset) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      {:error, _changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
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
        TableManager.new_table(String.to_atom(source["token"]))

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
    source = Repo.get(Source, source_id)

    table_id = String.to_atom(source.token)
    logs = SourceData.get_logs(table_id)
    render(conn, "show.html", logs: logs, source: source, public_token: nil)
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
        logs = SourceData.get_logs(table_id)
        render(conn, "show.html", logs: logs, source: source, public_token: public_token)
    end
  end

  def edit(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    user_id = conn.assigns.user.id
    changeset = Source.changeset(source, %{})
    disabled_source = source.token
    avg_rate = SourceData.get_avg_rate(String.to_atom(source.token))

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          overflow_source: s.overflow_source
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.load(source.token)
        s = Map.put(source, :token, token)

        if disabled_source == token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    render(conn, "edit.html",
      changeset: changeset,
      source: source,
      sources: sources,
      avg_rate: avg_rate
    )
  end

  def update(conn, %{"id" => source_id, "source" => source}) do
    old_source = Repo.get(Source, source_id)
    changeset = Source.changeset(old_source, source)
    user_id = conn.assigns.user.id
    disabled_source = old_source.token
    avg_rate = SourceData.get_avg_rate(String.to_atom(old_source.token))

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          overflow_source: s.overflow_source
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.load(source.token)
        s = Map.put(source, :token, token)

        if disabled_source == token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    case Repo.update(changeset) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html",
          changeset: changeset,
          source: old_source,
          sources: sources,
          avg_rate: avg_rate
        )
    end
  end

  def delete(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)

    case :ets.first(String.to_atom(source.token)) do
      :"$end_of_table" ->
        source |> Repo.delete!()
        {:ok, _table} = TableManager.delete_table(String.to_atom(source.token))

        conn
        |> put_flash(:info, "Source deleted!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      {timestamp, _unique_int, _monotime} ->
        now = System.os_time(:microsecond)

        if now - timestamp < 3_600_000_000 do
          source |> Repo.delete!()
          {:ok, _table} = TableManager.delete_table(String.to_atom(source.token))

          conn
          |> put_flash(:info, "Source deleted!")
          |> redirect(to: Routes.source_path(conn, :dashboard))
        else
          conn
          |> put_flash(
            :error,
            "Failed! Recent events found. Latest event must be greater than 24 hours old."
          )
          |> redirect(to: Routes.source_path(conn, :dashboard))
        end
    end
  end

  def clear_logs(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    {:ok, _table} = TableManager.delete_table(String.to_atom(source.token))

    conn
    |> put_flash(:info, "Logs cleared!")
    |> redirect(to: Routes.source_path(conn, :show, source_id))
  end
end
