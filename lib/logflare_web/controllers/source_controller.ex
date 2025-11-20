defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.Billing
  alias Logflare.Google.BigQuery
  alias Logflare.JSON
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Repo
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.SlackHookServer
  alias Logflare.Sources.Source.WebhookNotificationServer
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias LogflareWeb.AuthController
  alias Logflare.Backends

  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]

  defp env_dataset_id_append,
    do: Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  @lql_dialect :routing

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
        oauth_params = get_session(conn, :oauth_params)
        vercel_setup_params = get_session(conn, :vercel_setup)

        cond do
          oauth_params ->
            conn
            |> put_flash(:info, "Source created!")
            |> AuthController.redirect_for_oauth(user)

          vercel_setup_params ->
            conn
            |> put_flash(:info, "Source created!")
            |> AuthController.redirect_for_vercel(user)

          true ->
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

  def render_show_with_assigns(conn, _user, source, avg_rate) when avg_rate <= 5 do
    search_tip = SearchUtils.gen_search_tip()

    render(
      conn,
      "show.html",
      logs: get_and_encode_logs(source),
      source: source,
      public_token: source.public_token,
      search_tip: search_tip
    )
  end

  def render_show_with_assigns(conn, _user, source, avg_rate) when avg_rate > 5 do
    search_tip = SearchUtils.gen_search_tip()

    search_path =
      Routes.live_path(conn, LogflareWeb.Source.SearchLV, source,
        querystring: "c:count(*) c:group_by(t::minute)",
        tailing?: true
      )

    message = [
      "This source is seeing more than 5 events per second. ",
      Phoenix.HTML.Link.link("Search",
        to: "#{search_path}"
      ),
      " to see the latest events. Use the explore link to view in Google Data Studio."
    ]

    conn
    |> put_flash(
      :info,
      message
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
        to: ~p"/billing/edit"
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
    bigquery_project_id = user.bigquery_project_id || env_project_id()
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> env_dataset_id_append()

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
    bigquery_project_id = user.bigquery_project_id || env_project_id()
    dataset_id = user.bigquery_dataset_id || Integer.to_string(user.id) <> env_dataset_id_append()

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

  def public(%{assigns: %{user: _user, source: source}} = conn, %{"public_token" => _public_token}) do
    avg_rate = source.metrics.avg

    render_show_with_assigns(conn, conn.assigns.user, source, avg_rate)
  end

  def edit(%{assigns: %{source: source, plan: plan}} = conn, _params) do
    changeset = Source.update_by_user_changeset(source, %{})

    render(conn, "edit.html",
      plan: plan,
      changeset: changeset,
      source: source,
      notifications_opts: notifications_options()
    )
  end

  defp notifications_options do
    env = Application.get_env(:logflare, :env)

    plans =
      if env == :dev || :staging do
        Billing.list_plans() ++
          [Billing.legacy_plan()] ++ [%Billing.Plan{limit_alert_freq: 60_000}]
      else
        Billing.list_plans() ++ [Billing.legacy_plan()]
      end
      |> Enum.sort_by(& &1.limit_alert_freq, :desc)

    for p <- plans do
      limit = p.limit_alert_freq
      interval = Timex.Duration.from_milliseconds(limit)
      label = Timex.format_duration(interval, :humanized)

      [key: label, value: limit]
    end
    |> Enum.dedup()
  end

  def test_alerts(conn, %{"id" => source_id}) do
    with source = %Source{} <- Sources.get_by_and_preload(id: source_id),
         {:ok, %Tesla.Env{} = _response} <- WebhookNotificationServer.test_post(source) do
      conn
      |> put_flash(:info, "Webhook test successful!")
      |> redirect(to: Routes.source_path(conn, :edit, source.id))
    else
      nil ->
        conn
        |> put_flash(:error, "Source not found")
        |> redirect(to: Routes.source_path(conn, :index))

      {:error, %Tesla.Env{} = response} ->
        conn
        |> put_flash(
          :error,
          "Webhook test failed! Response status code was #{response.status}."
        )
        |> redirect(to: Routes.source_path(conn, :edit, source_id))

      {:error, response} ->
        conn
        |> put_flash(:error, "Webhook test failed! Error response: #{response}")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))
    end
  end

  def test_slack_hook(conn, %{"id" => source_id}) do
    with source = %Source{} <- Sources.get_by_and_preload(id: source_id),
         {:ok, %Tesla.Env{} = _response} <- SlackHookServer.test_post(source) do
      conn
      |> put_flash(:info, "Slack hook test successful!")
      |> redirect(to: Routes.source_path(conn, :edit, source.id))
    else
      nil ->
        conn
        |> put_flash(:error, "Source not found")
        |> redirect(to: Routes.source_path(conn, :index))

      {:error, %Tesla.Env{} = response} ->
        conn
        |> put_flash(
          :error,
          "Slack hook test failed! Response status code was #{response.status}."
        )
        |> redirect(to: Routes.source_path(conn, :edit, source_id))
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
        %{assigns: %{source: source, user: _user, plan: _plan}} = conn,
        %{"source" => %{"drop_lql_string" => lqlstring} = params}
      ) do
    with source_schema <- SourceSchemas.get_source_schema_by(source_id: source.id),
         schema <- Map.get(source_schema, :bigquery_schema),
         {:ok, lql_rules} <- Lql.Parser.parse(lqlstring, schema),
         {:warnings, nil} <-
           {:warnings, Lql.Rules.get_lql_parser_warnings(lql_rules, dialect: @lql_dialect)},
         params <- Map.put(params, "drop_lql_filters", lql_rules),
         {:ok, _changeset} <- Sources.update_source_by_user(source, params) do
      conn
      |> put_flash(:info, "Source updated!")
      |> redirect(to: Routes.source_path(conn, :edit, source.id))
    else
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

      {:error, :field_not_found, _suggested_querystring, error} ->
        conn
        |> put_flash(:error, error)
        |> redirect(to: Routes.source_path(conn, :edit, source.id))

      e ->
        Logger.error("Error updating dropped LQL.", error_string: inspect(e))

        conn
        |> put_flash(:error, "Something else went wrong. Contact support if this continues.")
        |> redirect(to: Routes.source_path(conn, :edit, source.id))
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
            to: ~p"/billing/edit"
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

  def update(%{assigns: %{source: old_source}} = conn, %{"source" => source_params}) do
    case Sources.update_source_by_user(old_source, source_params) do
      {:ok, source} ->
        :ok = Supervisor.ensure_started(source)

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
    now = DateTime.utc_now()

    {:ok, timestamp} = Backends.fetch_latest_timestamp(source) |> DateTime.from_unix(:microsecond)

    if DateTime.diff(now, timestamp, :millisecond) > :timer.hours(24) do
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
  end

  def del_source_and_redirect(%{assigns: %{source: source}} = conn, _params) do
    Supervisor.delete_source(source.token)

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

  defp get_and_encode_logs(%Source{} = source) do
    log_events = Backends.list_recent_logs(source)

    for le <- log_events, le do
      le
      |> Map.take([:body, :via_rule, :origin_source_id])
      |> case do
        %{body: %{"metadata" => %{"level" => level}}}
        when level in ~W(debug info warning error alert critical notice emergency) ->
          body = Map.put(le.body, "level", level)
          %{le | body: body}

        le ->
          le
      end
    end
  end

  defp put_flash_and_redirect_to_dashboard(conn, flash_level, flash_message) do
    conn
    |> put_flash(flash_level, flash_message)
    |> redirect(to: ~p"/dashboard")
  end
end
