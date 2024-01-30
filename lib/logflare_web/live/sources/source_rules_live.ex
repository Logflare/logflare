defmodule LogflareWeb.Sources.RulesLV do
  @moduledoc """
  Source Rule LV form edit
  """
  require Logger
  use LogflareWeb, :live_view

  alias Logflare.Lql
  alias Logflare.Rule
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Users
  alias LogflareWeb.RuleView

  @lql_dialect :routing
  @lql_string ""

  @impl true
  def render(assigns) do
    RuleView.render("source_rules.html", assigns)
  end

  @impl true
  def mount(%{"source_id" => source_id}, %{"user_id" => user_id}, socket) do
    user = Users.get_by_and_preload(id: user_id)
    source = Sources.get_by_and_preload(id: source_id)

    user =
      if user.admin do
        Users.get_by_and_preload(id: source.user_id)
      else
        user
      end

    sources =
      for s <- user.sources do
        if s.token == source.token do
          Map.put(s, :disabled, true)
        else
          Map.put(s, :disabled, false)
        end
      end

    socket =
      socket
      |> assign(:source, source)
      |> assign(:rules, source.rules)
      |> assign(:sources, sources)
      |> assign(:active_modal, nil)
      |> assign(:lql_string, @lql_string)
      |> assign(:error_message, nil)
      |> assign(:show_modal, false)
      |> clear_flash(:warning)

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  @impl true
  def handle_event("fsubmit", %{"rule" => rule_params}, socket) do
    %{source: source, rules: rules} = socket.assigns

    lqlstring = rule_params["lql_string"]

    socket =
      with source_schema <- SourceSchemas.get_source_schema_by(source_id: source.id),
           schema <- Map.get(source_schema, :bigquery_schema),
           {:ok, lql_rules} <- Lql.Parser.parse(lqlstring, schema),
           {:warnings, nil} <-
             {:warnings, Lql.Utils.get_lql_parser_warnings(lql_rules, dialect: @lql_dialect)} do
        rule_params = Map.put(rule_params, "lql_filters", lql_rules)

        case Rules.create_rule(rule_params, source) do
          {:ok, rule} ->
            socket
            |> assign(:rules, [rule | rules])
            |> put_flash(:info, "LQL source routing rule created successfully!")

          {:error, changeset} ->
            error_message = Rule.changeset_error_to_string(changeset)
            put_flash(socket, :error, error_message)
        end
      else
        {:error, :field_not_found, _suggested_querystring, error} ->
          put_flash(socket, :error, error)

        {:error, error} ->
          put_flash(socket, :error, error)

        {:warnings, warning} ->
          put_flash(socket, :info, warning)
      end

    {:noreply, socket}
  end

  def handle_event("delete_rule", %{"rule_id" => rule_id}, socket) do
    source = socket.assigns.source

    rule_id
    |> String.to_integer()
    |> Rules.delete_rule!()

    source = Sources.get_by_and_preload(token: source.token)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:rules, source.rules)

    {:noreply, socket}
  end
end
