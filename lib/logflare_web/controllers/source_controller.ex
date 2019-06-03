defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  plug LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create]

  plug LogflareWeb.Plugs.SetVerifySource
       when action in [:show, :edit, :update, :delete, :clear_logs, :favorite]

  alias Logflare.{Source, Sources, Repo, Users, SourceData, SourceManager, Google.BigQuery}
  alias Logflare.Logs.RejectedEvents
  alias LogflareWeb.AuthController

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  def dashboard(conn, _params) do
    sources =
      conn.assigns.user.sources
      |> Enum.map(&Sources.preload_defaults/1)

    render(conn, "dashboard.html",
      sources: sources,
      user_email: conn.assigns.user.email
    )
  end

  def favorite(conn, _params) do
    %{user: user, source: source} = conn.assigns

    {flash_key, message} =
      source
      |> Source.update_by_user_changeset(%{"favorite" => !source.favorite})
      |> Repo.update()
      |> case do
        {:ok, _source} ->
          Users.Cache.delete_cache_key_by_id(user.id)
          {:info, "Source updated!"}

        {:error, _changeset} ->
          {:error, "Something went wrong!"}
      end

    put_flash_and_redirect_to_dashboard(conn, flash_key, message)
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

        Users.Cache.delete_cache_key_by_id(user.id)

        if get_session(conn, :oauth_params) do
          conn
          |> put_flash(:info, "Source created!")
          |> AuthController.redirect_for_oauth(user)
        else
          put_flash_and_redirect_to_dashboard(conn, :info, "Source created!")
        end

      {:error, changeset} ->
        conn
        |> put_status(406)
        |> put_flash(:error, "Something went wrong!")
        |> assign(:changeset, changeset)
        |> redirect(to: Routes.source_path(conn, :new))
    end
  end

  def show(%{assigns: %{user: user, source: source}} = conn, _params) do
    render_show_with_assigns(conn, user, source)
  end

  def render_show_with_assigns(conn, user, source) do
    bigquery_project_id = user && (user.bigquery_project_id || @project_id)

    explore_link =
      bigquery_project_id &&
        generate_explore_link(user.id, user.email, source.token, bigquery_project_id)

    render(
      conn,
      "show.html",
      logs: get_and_encode_logs(source),
      source: source,
      public_token: source.public_token,
      explore_link: explore_link || ""
    )
  end

  def public(conn, %{"public_token" => public_token}) do
    Sources.Cache.get_by(public_token: public_token)
    |> case do
      %Source{} = source ->
        render_show_with_assigns(conn, conn.assigns.user, source)

      _ ->
        conn
        |> put_flash(:error, "Public path not found!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  def edit(conn, _params) do
    source = conn.assigns.source
    changeset = Source.update_by_user_changeset(source, %{})
    avg_rate = SourceData.get_avg_rate(source.token)

    render(conn, "edit.html",
      changeset: changeset,
      source: source,
      sources: conn.assigns.user.sources,
      avg_rate: avg_rate
    )
  end

  def update(conn, %{"source" => source_params}) do
    %{source: old_source, user: user} = conn.assigns
    # FIXME: Restricted params are filtered without notice
    changeset = Source.update_by_user_changeset(old_source, source_params)

    avg_rate = SourceData.get_avg_rate(old_source.token)

    sources =
      user.sources
      |> Enum.map(&Map.put(&1, :disabled, old_source.token === &1.token))

    case Repo.update(changeset) do
      {:ok, source} ->
        Users.Cache.delete_cache_key_by_id(user.id)
        ttl = source_params["bigquery_table_ttl"]

        if ttl do
          BigQuery.patch_table_ttl(
            source.token,
            String.to_integer(ttl) * 86_400_000,
            user.bigquery_project_id
          )
        end

        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, changeset} ->
        conn
        |> put_status(406)
        |> put_flash(:error, "Something went wrong!")
        |> render(
          "edit.html",
          changeset: changeset,
          source: old_source,
          sources: sources,
          avg_rate: avg_rate
        )
    end
  end

  def delete(conn, %{"id" => id}) do
    source = Sources.get_by(id: id)
    token = source.token

    cond do
      :ets.info(token) == :undefined ->
        del_source_and_redirect_with_info(conn, source)

      :ets.first(token) == :"$end_of_table" ->
        {:ok, _table} = SourceManager.delete_table(source.token)
        del_source_and_redirect_with_info(conn, source)

      {timestamp, _unique_int, _monotime} = :ets.first(source.token) ->
        now = System.os_time(:microsecond)

        if now - timestamp > 3_600_000_000 do
          {:ok, _table} = SourceManager.delete_table(source.token)
          del_source_and_redirect_with_info(conn, source)
        else
          put_flash_and_redirect_to_dashboard(
            conn,
            :error,
            "Failed! Recent events found. Latest event must be greater than 24 hours old."
          )
        end
    end
  end

  def clear_logs(%{assigns: %{source: source}} = conn, _params) do
    {:ok, _table} = SourceManager.reset_table(source.token)

    conn
    |> put_flash(:info, "Logs cleared!")
    |> redirect(to: Routes.source_path(conn, :show, source.id))
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
        "tableId" => BigQuery.GenUtils.format_table_name(source_id),
        "datasetId" => dataset_id,
        # billingProjectId" => billing_project_id,
        "connectorType" => "BIG_QUERY",
        "sqlType" => "STANDARD_SQL"
      })

    explore_link_prefix = "https://datastudio.google.com/explorer?authuser=#{user_email}&config="

    explore_link_prefix <> URI.encode(explore_link_config)
  end

  def rejected_logs(conn, %{"id" => id}) do
    source = Sources.Cache.get_by(id: id)

    render(
      conn,
      "show_rejected.html",
      logs: RejectedEvents.get_by_source(source),
      source: source
    )
  end

  defp maybe_encode_log_metadata(%{metadata: m} = log) do
    %{log | metadata: Jason.encode!(m, pretty: true)}
  end

  defp maybe_encode_log_metadata(log), do: log

  defp get_and_encode_logs(%Source{} = source) do
    source.token
    |> SourceData.get_logs()
    |> Enum.map(&maybe_encode_log_metadata/1)
  end

  defp del_source_and_redirect_with_info(conn, source) do
    Repo.delete!(source)

    Users.Cache.delete_cache_key_by_id(conn.assigns.user.id)
    put_flash_and_redirect_to_dashboard(conn, :info, "Source deleted!")
  end

  defp put_flash_and_redirect_to_dashboard(conn, flash_level, flash_message) do
    conn
    |> put_flash(flash_level, flash_message)
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end
end
