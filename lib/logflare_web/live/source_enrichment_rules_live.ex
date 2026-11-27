defmodule LogflareWeb.SourceEnrichmentRulesLive do
  @moduledoc false
  use LogflareWeb, :live_view

  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias Logflare.EnrichmentRules
  alias Logflare.EnrichmentRules.EnrichmentRule
  alias Logflare.Sources

  def render(assigns) do
    ~H"""
    <div class="my-4">
      <ul :if={@rules != []} id="enrichment-rules" class="list-unstyled">
        <li :for={rule <- @rules} id={"enrichment-rule-#{rule.id}"} class="tw-flex tw-flex-row tw-items-center tw-gap-4 tw-mb-2">
          <code>{format_conditions(rule)}</code>
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
          <label for="enrichment-rule-match">Match</label>
          <select id="enrichment-rule-match" name="rule[match]" class="form-control form-control-margin">
            <option value="all" selected={@match == "all"}>All conditions</option>
            <option value="any" selected={@match == "any"}>Any condition</option>
          </select>
        </div>
        <div class="form-group">
          <label for="enrichment-rule-conditions">Conditions</label>
          <textarea id="enrichment-rule-conditions" name="rule[conditions]" class="form-control form-control-margin tw-h-[80px]" placeholder="debug_mode equals errors_only\nm.response.status_code less_than 400">{@conditions}</textarea>
          <small class="form-text text-muted">
            One <code>field predicate value</code>
            per line. Predicates: {Enum.join(EnrichmentRule.predicates(), ", ")}. Separate <code>in</code>
            and <code>not_in</code>
            values with commas. <code>exists</code>, <code>not_exists</code>, <code>is_empty</code>
            and <code>not_empty</code>
            take no value. Add <code>/i</code>
            to a predicate to ignore case, as in <code>not_in/i</code>. <code>@severity_text</code>
            is the event's severity as ClickHouse derives it.
          </small>
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
      |> assign_form("all", "", "")
      |> refresh_rules()

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event("create", %{"rule" => params}, %{assigns: %{source: source}} = socket) do
    match = Map.get(params, "match", "all")
    conditions_text = Map.get(params, "conditions", "")
    set_fields_text = Map.get(params, "set_fields", "")

    socket =
      with {:ok, conditions} <- parse_conditions(conditions_text),
           {:ok, set_fields} <- parse_set_fields(set_fields_text),
           {:ok, _rule} <-
             EnrichmentRules.create(source, %{
               "match" => match,
               "conditions" => conditions,
               "set_fields" => set_fields
             }) do
        socket
        |> assign_form("all", "", "")
        |> refresh_rules()
        |> put_flash(:info, "Enrichment rule created.")
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          socket
          |> assign_form(match, conditions_text, set_fields_text)
          |> put_flash(:error, stringify_changeset_errors(changeset))

        {:error, message} ->
          socket
          |> assign_form(match, conditions_text, set_fields_text)
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

  defp assign_form(socket, match, conditions, set_fields) do
    assign(socket, match: match, conditions: conditions, set_fields: set_fields)
  end

  defp format_conditions(%{match: match, conditions: conditions}) do
    joiner = if match == :any, do: " OR ", else: " AND "
    Enum.map_join(conditions, joiner, &format_condition/1)
  end

  defp format_condition(%{"path" => path, "predicate" => predicate} = condition) do
    predicate = if condition["case_insensitive"], do: predicate <> "/i", else: predicate

    case Map.fetch(condition, "value") do
      {:ok, value} -> "#{path} #{predicate} #{Jason.encode!(value)}"
      :error -> "#{path} #{predicate}"
    end
  end

  defp parse_conditions(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, []}, fn line, {:ok, acc} ->
      case parse_condition(line) do
        {:ok, condition} ->
          {:cont, {:ok, [condition | acc]}}

        :error ->
          {:halt, {:error, "Conditions must be one field predicate value per line: #{line}"}}
      end
    end)
    |> then(fn
      {:ok, conditions} -> {:ok, Enum.reverse(conditions)}
      error -> error
    end)
  end

  defp parse_condition(line) do
    case String.split(line, ~r/\s+/, parts: 3) do
      [path, predicate | value] ->
        {:ok, path |> base_condition(predicate) |> put_condition_value(value)}

      _ ->
        :error
    end
  end

  defp base_condition(path, predicate) do
    case String.split(predicate, "/", parts: 2) do
      [name, "i"] -> %{"path" => path, "predicate" => name, "case_insensitive" => true}
      _ -> %{"path" => path, "predicate" => predicate}
    end
  end

  defp put_condition_value(condition, []), do: condition

  defp put_condition_value(%{"predicate" => predicate} = condition, [values])
       when predicate in ["in", "not_in"] do
    parsed = values |> String.split(",") |> Enum.map(&parse_value(String.trim(&1)))
    Map.put(condition, "value", parsed)
  end

  defp put_condition_value(condition, [value]),
    do: Map.put(condition, "value", parse_value(value))

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
