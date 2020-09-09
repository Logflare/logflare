defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  plug LogflareWeb.Plugs.CheckSourceCount when action in [:create, :del_source_and_redirect]

  alias Logflare.JSON
  alias Logflare.{Source, Sources, Repo, Google.BigQuery, TeamUsers, Teams, Plans}
  alias Logflare.Source.{Supervisor, WebhookNotificationServer, SlackHookServer}
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Logs.{RejectedLogEvents, Search}
  alias LogflareWeb.AuthController

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  def api_index(%{assigns: %{user: user}} = conn, _params) do
    sources = preload_sources_for_dashboard(user.sources)

    conn |> json(sources)
  end

  def dashboard(%{assigns: %{user: user, team_user: team_user, team: _team}} = conn, _params) do
    sources = preload_sources_for_dashboard(user.sources)

    home_team = Teams.get_home_team!(team_user)

    team_users_with_teams =
      TeamUsers.list_team_users_by_and_preload(provider_uid: team_user.provider_uid)

    render(conn, "dashboard.html",
      sources: sources,
      home_team: home_team,
      team_users: team_users_with_teams,
      current_node: Node.self()
    )
  end

  def dashboard(%{assigns: %{user: user, team: team}} = conn, _params) do
    sources = preload_sources_for_dashboard(user.sources)

    home_team = team

    team_users_with_teams = TeamUsers.list_team_users_by_and_preload(email: user.email)

    render(conn, "dashboard.html",
      sources: sources,
      home_team: home_team,
      team_users: team_users_with_teams,
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

  def create(%{assigns: %{user: user}} = conn, %{"source" => source_params}) do
    source_params
    |> Map.put("token", Ecto.UUID.generate())
    |> Sources.create_source(user)
    |> case do
      {:ok, source} ->
        if get_session(conn, :oauth_params) do
          conn
          |> put_flash(:info, "Source created!")
          |> AuthController.redirect_for_oauth(user)
        else
          conn
          |> put_flash(:info, "Source created!")
          |> redirect(to: Routes.source_path(conn, :show, source.id, new: true))
        end

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> assign(:changeset, changeset)
        |> render("new.html", changeset: changeset)
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

  def explore(%{assigns: %{plan: %{name: "Free"}, source: source}} = conn, _params) do
    message = [
      "Please ",
      Phoenix.HTML.Link.link("upgrade to explore",
        to: "#{Routes.billing_path(conn, :edit)}"
      ),
      " in Google Data Studio."
    ]

    explore_error(conn, source, message)
  end

  def explore(
        %{assigns: %{team_user: %{provider: "google"} = team_user, user: user, source: source}} =
          conn,
        _params
      ) do
    bigquery_project_id = user.bigquery_project_id || @project_id
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> @dataset_id_append

    explore_link =
      generate_explore_link(team_user.email, source.token, bigquery_project_id, dataset_id)

    conn
    |> redirect(external: explore_link)
  end

  def explore(
        %{assigns: %{team_user: _team_user, user: _user, source: source}} = conn,
        _params
      ) do
    message = [
      Phoenix.HTML.Link.link("Sign in with Google",
        to: "#{Routes.oauth_path(conn, :request, "google")}"
      ),
      " to explore in Data Studio."
    ]

    explore_error(conn, source, message)
  end

  def explore(%{assigns: %{user: %{provider: "google"} = user, source: source}} = conn, _params) do
    bigquery_project_id = user.bigquery_project_id || @project_id
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> @dataset_id_append

    explore_link =
      generate_explore_link(user.email, source.token, bigquery_project_id, dataset_id)

    conn
    |> redirect(external: explore_link)
  end

  def explore(%{assigns: %{user: _user, source: source}} = conn, _params) do
    message = [
      Phoenix.HTML.Link.link("Sign in with Google",
        to: "#{Routes.oauth_path(conn, :request, "google")}"
      ),
      " to explore in Data Studio."
    ]

    explore_error(conn, source, message)
  end

  defp explore_error(conn, source, message) do
    conn
    |> put_flash(:error, message)
    |> redirect(to: Routes.source_path(conn, :show, source.id))
  end

  def public(%{assigns: %{user: _user, source: source}} = conn, %{"public_token" => public_token}) do
    avg_rate = source.metrics.avg
    render_show_with_assigns(conn, conn.assigns.user, source, avg_rate)
  end

  def edit(%{assigns: %{source: source}} = conn, _params) do
    changeset = Source.update_by_user_changeset(source, %{})

    render(conn, "edit.html",
      changeset: changeset,
      source: source,
      notifications_opts: notifications_options()
    )
  end

  defp notifications_options() do
    env = Application.get_env(:logflare, :env)

    plans =
      if env == :dev || :staging do
        Plans.list_plans() ++ [Plans.legacy_plan()] ++ [%Plans.Plan{limit_alert_freq: 60_000}]
      else
        Plans.list_plans() ++ [Plans.legacy_plan()]
      end

    for p <- plans do
      limit = p.limit_alert_freq
      interval = Timex.Duration.from_milliseconds(limit)
      label = Timex.format_duration(interval, :humanized)

      [key: label, value: limit]
    end
    |> Enum.dedup()
  end

  def test_alerts(conn, %{"id" => source_id}) do
    source = Sources.get_by_and_preload(id: source_id)

    case WebhookNotificationServer.test_post(source) do
      {:ok, %Tesla.Env{} = _response} ->
        conn
        |> put_flash(:info, "Webhook test successful!")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, %Tesla.Env{} = response} ->
        conn
        |> put_flash(:error, "Webhook test failed! Response status code was #{response.status}.")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, response} ->
        conn
        |> put_flash(:error, "Webhook test failed! Error response: #{response}")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      _else ->
        conn
        |> put_flash(
          :error,
          "Webhook test failed! Unknown error. Please contact support if this continues."
        )
        |> redirect(to: Routes.source_path(conn, :edit, source.id))
    end
  end

  def test_slack_hook(conn, %{"id" => source_id}) do
    source = Sources.get_by_and_preload(id: source_id)

    case SlackHookServer.test_post(source) do
      {:ok, %Tesla.Env{} = _response} ->
        conn
        |> put_flash(:info, "Slack hook test successful!")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, %Tesla.Env{} = response} ->
        conn
        |> put_flash(
          :error,
          "Slack hook test failed! Response status code was #{response.status}."
        )
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, response} ->
        conn
        |> put_flash(:error, "Slack hook test failed! Error response: #{response}")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))
    end
  end

  def delete_slack_hook(conn, %{"id" => source_id}) do
    Repo.get(Source, source_id)
    |> Sources.delete_slack_hook_url()
    |> case do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Slack hook deleted!")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))

      {:error, _changeset} ->
        conn
        |> put_flash(:error, "Delete failed! Contact support if this continues.")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))
    end
  end

  def update(
        %{assigns: %{source: source, user: _user, plan: plan}} = conn,
        %{"source" => %{"notifications_every" => _freq} = params}
      ) do
    case Sources.update_source_by_user(source, plan, params) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, :upgrade} ->
        message = [
          "Please ",
          Phoenix.HTML.Link.link("upgrade",
            to: "#{Routes.billing_path(conn, :edit)}"
          ),
          " first!"
        ]

        conn
        |> put_flash(:error, message)
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      {:error, changeset} ->
        conn
        |> put_status(406)
        |> put_flash(:error, "Something went wrong!")
        |> render(
          "edit.html",
          changeset: changeset,
          source: source,
          notifications_opts: notifications_options()
        )
    end
  end

  def update(%{assigns: %{source: old_source, user: user}} = conn, %{"source" => source_params}) do
    changeset = Source.update_by_user_changeset(old_source, source_params)

    case Repo.update(changeset) do
      {:ok, source} ->
        ttl = source.bigquery_table_ttl

        if ttl do
          BigQuery.patch_table_ttl(
            source.token,
            source.bigquery_table_ttl * 86_400_000,
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
          notifications_opts: notifications_options()
        )
    end
  end

  def delete(%{assigns: %{source: source}} = conn, params) do
    token = source.token

    cond do
      :ets.info(token) == :undefined ->
        del_source_and_redirect(conn, params)

      :ets.first(token) == :"$end_of_table" ->
        del_source_and_redirect(conn, params)

      {timestamp, _unique_int, _monotime} = :ets.first(token) ->
        now = System.os_time(:microsecond)

        if now - timestamp > 3_600_000_000 do
          del_source_and_redirect(conn, params)
        else
          message = [
            "Failed! Recent events are less than 24 hours old. ",
            Phoenix.HTML.Link.link("Force delete",
              to: "#{Routes.source_path(conn, :del_source_and_redirect, source.id)}",
              method: :delete
            ),
            " this source."
          ]

          put_flash_and_redirect_to_dashboard(conn, :error, message)
        end

      true ->
        del_source_and_redirect(conn, params)
    end
  end

  def del_source_and_redirect(%{assigns: %{source: source}} = conn, _params) do
    if :ets.info(source.token) != :undefined do
      Supervisor.delete_source(source.token)
    end

    case Sources.delete_source(source) do
      {:ok, _response} ->
        put_flash_and_redirect_to_dashboard(conn, :info, "Source deleted!")

      {:error, _response} ->
        put_flash_and_redirect_to_dashboard(
          conn,
          :error,
          "Something went wrong! Please try again later."
        )
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
      JSON.encode(%{
        "projectId" => project_id,
        "tableId" => BigQuery.GenUtils.format_table_name(source_id),
        "datasetId" => dataset_id,
        # billingProjectId" => billing_project_id,
        "connectorType" => "BIG_QUERY",
        "sqlType" => "STANDARD_SQL",
        "isPartitioned" => true
      })

    explore_link_prefix = "https://datastudio.google.com/explorer?authuser=#{user_email}&config="

    explore_link_prefix <> URI.encode(explore_link_config)
  end

  def rejected_logs(%{assigns: %{source: source}} = conn, %{"id" => _id}) do
    rejected_logs = RejectedLogEvents.get_by_source(source)
    render(conn, "show_rejected.html", logs: rejected_logs, source: source)
  end

  defp preload_sources_for_dashboard(sources) do
    sources
    |> Enum.map(&Sources.preload_defaults/1)
    |> Enum.map(&Sources.preload_saved_searches/1)
    |> Enum.map(&Sources.put_schema_field_count/1)
    |> Enum.sort_by(& &1.name, &<=/2)
    |> Enum.sort_by(& &1.favorite, &>=/2)
  end

  defp get_and_encode_logs(%Source{} = source) do
    log_events = RLS.list_for_cluster(source.token)

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

  defp put_flash_and_redirect_to_dashboard(conn, flash_level, flash_message) do
    conn
    |> put_flash(flash_level, flash_message)
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end
end
