defmodule LogflareWeb.Sources.RulesLV do
  @moduledoc """
  Source Rule LV form edit
  """
  require Logger
  use LogflareWeb, :live_view
  use LogflareWeb.ModalLiveHelpers

  alias LogflareWeb.RuleView
  alias Logflare.{Sources, Users}
  alias Logflare.{Rules, Rule}
  alias Logflare.Lql

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
      with {:ok, lql_rules} <- Lql.Parser.parse(lqlstring, source.source_schema.bigquery_schema),
           {:warnings, nil} <-
             {:warnings, Lql.Utils.get_lql_parser_warnings(lql_rules, dialect: @lql_dialect)} do
        rule_params = Map.put(rule_params, "lql_filters", lql_rules)

        case Rules.create_rule(rule_params, source) do
          {:ok, rule} ->
            socket
            |> assign(:has_regex_rules, Rules.has_regex_rules?(source.rules))
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

  @deprecated "Delete when all source rules are upgraded to LQL"
  def handle_event("upgrade_rules", _metadata, %{assigns: as} = socket) do
    socket =
      case Rules.upgrade_rules_to_lql(as.rules) do
        :ok ->
          source = Sources.get_by_and_preload(token: as.source.token)

          socket
          |> assign(:source, source)
          |> assign(:rules, source.rules)
          |> put_flash(:info, "Upgrade successfull!")

        {:error, changeset} ->
          error_message = Rule.changeset_error_to_string(changeset)
          put_flash(socket, :error, error_message)
      end

    {:noreply, socket}
  end
end
