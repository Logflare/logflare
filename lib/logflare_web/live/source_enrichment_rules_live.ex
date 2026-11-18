defmodule LogflareWeb.SourceEnrichmentRulesLive do
  @moduledoc false
  use LogflareWeb, :live_view

  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias Logflare.EnrichmentRules
  alias Logflare.Sources

  def render(assigns) do
    ~H"""
    <div class="my-4">
      <ul :if={@rules != []} id="enrichment-rules" class="list-unstyled">
        <li :for={rule <- @rules} id={"enrichment-rule-#{rule.id}"} class="tw-flex tw-flex-row tw-items-center tw-gap-4 tw-mb-2">
          <code>{rule.lql_string}</code>
          <span>&rarr;</span>
          <code>{format_set_fields(rule.set_fields)}</code>
          <.button phx-click="delete" phx-value-id={rule.id} variant="danger" data-confirm="Delete this enrichment rule?">
            Delete
          </.button>
        </li>
      </ul>
      <p :if={@rules == []}>No enrichment rules yet.</p>

      <.form id="enrichment-rule-form" for={%{}} as={:rule} action="#" phx-submit="create">
        <div class="form-group">
          <label for="enrichment-rule-lql">LQL filter</label>
          <input id="enrichment-rule-lql" name="rule[lql_string]" class="form-control form-control-margin" placeholder="debug_mode:errors_only -m.level:error" value={@lql_string} />
        </div>
        <div class="form-group">
          <label for="enrichment-rule-set-fields">Set fields</label>
          <textarea id="enrichment-rule-set-fields" name="rule[set_fields]" class="form-control form-control-margin tw-h-[80px]" placeholder="excluded_by_user_config=true">{@set_fields}</textarea>
          <small class="form-text text-muted">
            One <code>field=value</code> per line. Use <code>m.</code> as shorthand for <code>metadata.</code>. <code>true</code>, <code>false</code> and numbers are typed; anything else is a string.
          </small>
        </div>
        {submit("Add enrichment rule", class: "btn btn-primary form-button")}
      </.form>
    </div>
    """
  end

  def mount(_params, %{"source_id" => source_id}, socket) do
    socket =
      socket
      |> assign(:source, Sources.get(source_id))
      |> assign(:lql_string, "")
      |> assign(:set_fields, "")
      |> refresh_rules()

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event("create", %{"rule" => params}, %{assigns: %{source: source}} = socket) do
    lql_string = Map.get(params, "lql_string", "")
    set_fields_text = Map.get(params, "set_fields", "")

    socket =
      with {:ok, set_fields} <- parse_set_fields(set_fields_text),
           {:ok, _rule} <-
             EnrichmentRules.create(source, %{
               "lql_string" => lql_string,
               "set_fields" => set_fields
             }) do
        socket
        |> assign(:lql_string, "")
        |> assign(:set_fields, "")
        |> refresh_rules()
        |> put_flash(:info, "Enrichment rule created.")
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          socket
          |> assign(:lql_string, lql_string)
          |> assign(:set_fields, set_fields_text)
          |> put_flash(:error, stringify_changeset_errors(changeset))

        {:error, message} ->
          socket
          |> assign(:lql_string, lql_string)
          |> assign(:set_fields, set_fields_text)
          |> put_flash(:error, message)
      end

    {:noreply, socket}
  end

  def handle_event("delete", %{"id" => id}, %{assigns: %{source: source}} = socket) do
    socket =
      with {rule_id, ""} <- Integer.parse(id),
           %{source_id: source_id} = rule when source_id == source.id <-
             EnrichmentRules.get(rule_id),
           {:ok, _rule} <- EnrichmentRules.delete(rule) do
        socket
        |> refresh_rules()
        |> put_flash(:info, "Enrichment rule deleted.")
      else
        _ -> put_flash(socket, :error, "Enrichment rule not found on this source.")
      end

    {:noreply, socket}
  end

  defp refresh_rules(%{assigns: %{source: source}} = socket) do
    assign(socket, :rules, EnrichmentRules.list_by_source_id(source.id))
  end

  defp format_set_fields(set_fields) do
    set_fields
    |> Enum.sort()
    |> Enum.map_join(", ", fn {path, value} -> "#{path}=#{Jason.encode!(value)}" end)
  end

  defp parse_set_fields(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, %{}}, fn line, {:ok, acc} ->
      case String.split(line, "=", parts: 2) do
        [path, value] when path != "" ->
          {:cont, {:ok, Map.put(acc, String.trim(path), parse_value(String.trim(value)))}}

        _ ->
          {:halt, {:error, "Set fields must be one field=value per line: #{line}"}}
      end
    end)
  end

  defp parse_value("true"), do: true
  defp parse_value("false"), do: false

  defp parse_value(value) do
    with :error <- parse_number(value) do
      value |> String.trim_leading("\"") |> String.trim_trailing("\"")
    end
  end

  defp parse_number(value) do
    case {Integer.parse(value), Float.parse(value)} do
      {{int, ""}, _} -> int
      {_, {float, ""}} -> float
      _ -> :error
    end
  end
end
