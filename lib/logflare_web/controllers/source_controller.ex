defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create]

  plug LogflareWeb.Plugs.VerifySourceOwner
       when action in [:show, :update, :delete, :clear_logs, :favorite]

  alias Logflare.{Source, Users, Sources, Repo, SourceData, SourceManager, Google.BigQuery, User}
  alias Logflare.Logs.RejectedEvents
  alias LogflareWeb.AuthController

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  def dashboard(conn, _params) do
    sources =
      conn.assigns.user
      |> Users.get_sources()
      |> Enum.map(&Source.update_metrics_latest/1)

    user_email = conn.assigns.user.email

    render(conn, "dashboard.html", sources: sources, user_email: user_email)
  end

  def favorite(conn, %{"id" => source_pk}) do
    old_source = Sources.get_by_pk(source_pk)

    {flash_key, message} =
      old_source
      |> Source.update_by_user_changeset(%{"favorite" => not old_source.favorite})
      |> Repo.update()
      |> case do
        {:ok, _source} ->
          {:info, "Source updated!"}

        {:error, _changeset} ->
          {:error, "Something went wrong!"}
      end

    conn
    |> put_flash(flash_key, message)
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end

  def new(conn, _params) do
    changeset = Source.update_by_user_changeset(%Source{}, %{})
    render(conn, "new.html", changeset: changeset)
  end

  def create(%{assigns: %{user: user}} = conn, %{"source" => source}) do
    user
    |> Ecto.build_assoc(:sources)
    |> Source.update_by_user_changeset(source)
    |> Repo.insert()
    |> case do
      {:ok, source} ->
        spawn(fn ->
          SourceManager.new_table(source.token)
        end)

        if get_session(conn, :oauth_params) do
          conn
          |> put_flash(:info, "Source created!")
          |> AuthController.redirect_for_oauth(user)
        else
          conn
          |> put_flash(:info, "Source created!")
          |> redirect(to: Routes.source_path(conn, :dashboard))
        end

      {:error, changeset} ->
        conn
        |> put_status(406)
        |> put_flash(:error, "Something went wrong!")
        |> assign(:changeset, changeset)
        |> redirect(to: Routes.source_path(conn, :new))
    end
  end

  def show(%{assigns: %{user: user}} = conn, %{"id" => pk}) do
    source = Sources.get_by_pk(pk)
    bigquery_project_id = user.bigquery_project_id || @project_id
    explore_link = generate_explore_link(user.id, user.email, source.token, bigquery_project_id)

    logs =
      source.token
      |> SourceData.get_logs()
      |> Enum.map(&maybe_encode_log_metadata/1)

    render(conn, "show.html",
      logs: logs,
      source: source,
      public_token: nil,
      explore_link: explore_link
    )
  end

  def public(conn, %{"public_token" => public_token}) do
    explore_link = ""

    public_token
    |> Sources.Cache.get_by_public_token()
    |> case do
      %{token: token} = source ->
        logs =
          token
          |> SourceData.get_logs()
          |> Enum.map(&maybe_encode_log_metadata/1)

        render(conn, "show.html",
          logs: logs,
          source: source,
          public_token: public_token,
          explore_link: explore_link
        )

      _ ->
        conn
        |> put_flash(:error, "Public path not found!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  def update(conn, %{"id" => source_id, "source" => updated_params}) do
    old_source = Repo.get(Source, source_id)
    changeset = Source.update_by_user_changeset(old_source, updated_params)
    user_id = conn.assigns.user.id
    disabled_source = old_source.token
    avg_rate = SourceData.get_avg_rate(old_source.token)

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.Atom.load(source.token)
        s = Map.put(source, :token, token)

        if disabled_source == token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    case Repo.update(changeset) do
      {:ok, source} ->
        case updated_params do
          %{"bigquery_table_ttl" => ttl} ->
            %Logflare.User{bigquery_project_id: project_id} = Repo.get(User, user_id)

            ttl = String.to_integer(ttl) * 86_400_000
            BigQuery.patch_table_ttl(source.token, ttl, project_id)

          _ ->
            nil
        end

        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))

      {:error, changeset} ->
        conn
        |> put_status(406)
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

    case :ets.info(source.token) do
      :undefined ->
        source |> Repo.delete!()

        conn
        |> put_flash(:info, "Source deleted!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      _ ->
        case :ets.first(source.token) do
          :"$end_of_table" ->
            {:ok, _table} = SourceManager.delete_table(source.token)
            source |> Repo.delete!()

            conn
            |> put_flash(:info, "Source deleted!")
            |> redirect(to: Routes.source_path(conn, :dashboard))

          {timestamp, _unique_int, _monotime} ->
            now = System.os_time(:microsecond)

            if now - timestamp > 3_600_000_000 do
              {:ok, _table} = SourceManager.delete_table(source.token)
              source |> Repo.delete!()

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
  end

  def clear_logs(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    {:ok, _table} = SourceManager.reset_table(source.token)

    conn
    |> put_flash(:info, "Logs cleared!")
    |> redirect(to: Routes.source_path(conn, :show, source_id))
  end

  defp generate_explore_link(
         user_id,
         user_email,
         source_id,
         project_id
         # billing_project_id
       )
       when is_atom(source_id) do
    dataset_id = Integer.to_string(user_id) <> @dataset_id_append

    {:ok, explore_link_config} =
      Jason.encode(%{
        "projectId" => project_id,
        "tableId" => BigQuery.format_table_name(source_id),
        "datasetId" => dataset_id,
        # billingProjectId" => billing_project_id,
        "connectorType" => "BIG_QUERY",
        "sqlType" => "STANDARD_SQL"
      })

    explore_link_prefix = "https://datastudio.google.com/explorer?authuser=#{user_email}&config="

    explore_link_prefix <> URI.encode(explore_link_config)
  end

  defp maybe_encode_log_metadata(%{metadata: m} = log) do
    %{log | metadata: Jason.encode!(m, pretty: true)}
  end

  defp maybe_encode_log_metadata(log), do: log
end
