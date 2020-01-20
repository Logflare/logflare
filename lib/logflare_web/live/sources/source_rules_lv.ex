defmodule LogflareWeb.Sources.RulesLV do
  @moduledoc """
  Source Rule LV form edit
  """
  require Logger
  use Phoenix.LiveView

  alias LogflareWeb.RuleView
  alias Logflare.{Sources, Users}
  alias Logflare.{Rules, Rule}
  alias Logflare.Lql
  use LogflareWeb.LiveViewUtils
  @lql_dialect :routing
  @lql_string ""

  def render(assigns) do
    RuleView.render("index.html", assigns)
  end

  def mount(%{"user_id" => user_id}, socket) do
    user = Users.Cache.get_by_and_preload(id: user_id)

    socket =
      socket
      |> assign(:sources, user.sources)
      |> assign(:lql_string, @lql_string)
      |> assign(:error_message, nil)

    {:ok, socket}
  end

  def handle_params(%{"source_id" => source_id}, _uri, socket) do
    source =
      source_id
      |> String.to_integer()
      |> Sources.Cache.get_by_id_and_preload()

    sources =
      for s <- socket.assigns.sources do
        if s.token == source.token do
          Map.put(s, :disabled, true)
        else
          Map.put(s, :disabled, false)
        end
      end

    {:noreply, assign(socket, source: source, rules: source.rules, sources: sources)}
  end

  def handle_event("fsubmit", %{"rule" => rule_params}, socket) do
    schema = Sources.Cache.get_bq_schema(socket.assigns.source)
    lqls = rule_params["lql_string"]

    socket =
      with {:ok, lql_rules} <- Lql.Parser.parse(lqls, schema),
           {:warnings, nil} <-
             {:warnings, Lql.Utils.get_lql_parser_warnings(lql_rules, dialect: @lql_dialect)} do
        rule_params = Map.put(rule_params, "lql_filters", lql_rules)

        case Rules.create_rule(socket.assigns.source, rule_params) do
          {:ok, rule} ->
            assign(socket, :rules, [rule | socket.assigns.rules])

          {:error, changeset} ->
            error_message = Rule.changeset_error_to_string(changeset)
            assign(socket, :error_message, error_message)
        end
      else
        {:error, error} ->
          assign(socket, :error_message, error)

        {:warnings, error} ->
          assign(socket, :error_message, error)
      end

    {:noreply, socket}
  end

  def handle_event("delete_rule", %{"rule_id" => rule_id}, socket) do
    source = socket.assigns.source

    Rules.delete_rule!(String.to_integer(rule_id))

    source = Sources.get_by_and_preload(token: source.token)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:rules, source.rules)

    {:noreply, socket}
  end
end
