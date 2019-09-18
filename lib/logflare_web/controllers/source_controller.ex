defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  plug LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create]

  plug LogflareWeb.Plugs.SetVerifySource
       when action in [
              :show,
              :edit,
              :update,
              :delete,
              :clear_logs,
              :rejected_logs,
              :favorite,
              :search,
              :explore
            ]

  alias Logflare.{Source, Sources, Repo, Google.BigQuery}
  alias Logflare.Source.{Supervisor, Data}
  alias Logflare.Logs.{RejectedLogEvents, Search}
  alias LogflareWeb.AuthController

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  def dashboard(conn, _params) do
    sources =
      conn.assigns.user.sources
      |> Enum.map(&Sources.preload_defaults/1)
      |> Enum.map(&Sources.put_schema_field_count/1)
      |> Enum.sort_by(&if(&1.favorite, do: 1, else: 0), &>=/2)

    render(conn, "dashboard.html",
      sources: sources,
      user_email: conn.assigns.user.email,
      current_node: Node.self()
    )
  end

  def favorite(conn, _params) do
    %{user: _user, source: source} = conn.assigns

    {flash_key, message} =
      source
      |> Source.update_by_user_changeset(%{"favorite" => !source.favorite})
      |> Repo.update()
      |> case do
        {:ok, _source} ->
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
          Supervisor.new_source(source.token)
        end)

        if get_session(conn, :oauth_params) do
          conn
          |> put_flash(:info, "Source created!")
          |> AuthController.redirect_for_oauth(user)
        else
          put_flash_and_redirect_to_dashboard(conn, :info, "Source created!")
        end

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> assign(:changeset, changeset)
        |> redirect(to: Routes.source_path(conn, :new))
    end
  end

  def show(%{assigns: %{user: user, source: source}} = conn, _params) do
    render_show_with_assigns(conn, user, source, source.metrics.avg)
  end

  def render_show_with_assigns(conn, _user, source, avg_rate) when avg_rate <= 25 do
    search_tip = Search.Utils.gen_search_tip()

    render(
      conn,
      "show.html",
      logs: get_and_encode_logs(source),
      source: source,
      public_token: source.public_token,
      search_tip: search_tip
    )
  end

  def render_show_with_assigns(conn, _user, source, avg_rate) when avg_rate > 25 do
    search_tip = Search.Utils.gen_search_tip()

    conn
    |> put_flash(
      :info,
      "This source is seeing more than 25 events per second. Refresh to see the latest events. Use the explore link to view in Google Data Studio."
    )
    |> render(
      "show.html",
      logs: get_and_encode_logs(source),
      source: source,
      public_token: source.public_token,
      search_tip: search_tip
    )
  end

  def explore(%{assigns: %{user: user, source: source}} = conn, _params) do
    if user.provider == "google" do
      bigquery_project_id = user.bigquery_project_id || @project_id
      dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> @dataset_id_append

      explore_link =
        generate_explore_link(user.email, source.token, bigquery_project_id, dataset_id)

      conn
      |> redirect(external: explore_link)
    else
      conn
      |> put_flash(:error, "Sign in with Google to explore in Data Studio.")
      |> redirect(to: Routes.source_path(conn, :show, source.id))
    end
  end

  def search(%{assigns: %{user: user, source: source}} = conn, params) do
    tailing? =
      case params["tailing"] do
        "true" -> true
        "false" -> false
        _ -> nil
      end

    session = %{
      source: source,
      user: user,
      querystring: params["q"]
    }

    session = if not is_nil(tailing?), do: Map.put(session, :tailing?, tailing?), else: session

    live_render(conn, LogflareWeb.Source.SearchLV, session: session)
  end

  def public(conn, %{"public_token" => public_token}) do
    Sources.Cache.get_by(public_token: public_token)
    |> case do
      %Source{} = source ->
        avg_rate = source.metrics.avg
        render_show_with_assigns(conn, conn.assigns.user, source, avg_rate)

      _ ->
        conn
        |> put_flash(:error, "Public path not found!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  def edit(%{assigns: %{source: source}} = conn, _params) do
    changeset = Source.update_by_user_changeset(source, %{})

    render(conn, "edit.html",
      changeset: changeset,
      source: source,
      sources: conn.assigns.user.sources
    )
  end

  def update(conn, %{"source" => source_params}) do
    %{source: old_source, user: user} = conn.assigns
    # FIXME: Restricted params are filtered without notice
    changeset = Source.update_by_user_changeset(old_source, source_params)

    sources =
      user.sources
      |> Enum.map(&Map.put(&1, :disabled, old_source.token === &1.token))

    case Repo.update(changeset) do
      {:ok, source} ->
        ttl = source_params["bigquery_table_ttl"]

        if ttl do
          BigQuery.patch_table_ttl(
            source.token,
            String.to_integer(ttl) * 86_400_000,
            user.bigquery_dataset_id,
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
          sources: sources
        )
    end
  end

  def delete(%{assigns: %{source: source}} = conn, _conn) do
    token = source.token

    cond do
      :ets.info(token) == :undefined ->
        del_source_and_redirect_with_info(conn, source)

      :ets.first(token) == :"$end_of_table" ->
        {:ok, _table} = Supervisor.delete_source(source.token)
        del_source_and_redirect_with_info(conn, source)

      {timestamp, _unique_int, _monotime} = :ets.first(source.token) ->
        now = System.os_time(:microsecond)

        if now - timestamp > 3_600_000_000 do
          {:ok, _table} = Supervisor.delete_source(source.token)
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
    {:ok, _table} = Supervisor.reset_source(source.token)
    {:ok, true} = RejectedLogEvents.delete_by_source(source)

    conn
    |> put_flash(:info, "Logs cleared!")
    |> redirect(to: Routes.source_path(conn, :show, source.id))
  end

  defp generate_explore_link(
         user_email,
         source_id,
         project_id,
         dataset_id
         # billing_project_id
       )
       when is_atom(source_id) do
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

  def rejected_logs(%{assigns: %{source: source}} = conn, %{"id" => _id}) do
    rejected_logs = RejectedLogEvents.get_by_source(source)
    render(conn, "show_rejected.html", logs: rejected_logs, source: source)
  end

  defp get_and_encode_logs(%Source{} = source) do
    log_events = Data.get_logs_across_cluster(source.token)

    for le <- log_events, le do
      le =
        le
        |> Map.from_struct()
        |> Map.take([:body, :via_rule, :origin_source_id])
        |> Map.update!(:body, &Map.from_struct/1)

      if le.via_rule do
        %{le | via_rule: %{regex: le.via_rule.regex}}
      else
        le
      end
    end
  end

  defp del_source_and_redirect_with_info(conn, source) do
    Repo.delete!(source)

    put_flash_and_redirect_to_dashboard(conn, :info, "Source deleted!")
  end

  defp put_flash_and_redirect_to_dashboard(conn, flash_level, flash_message) do
    conn
    |> put_flash(flash_level, flash_message)
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end
end
