defmodule Logflare.EnrichmentRules do
  @moduledoc """
  Conditional enrichment rules. Each rule is an LQL filter on a source and the fields it sets on
  every event that matches. Rules are applied when events are dispatched, before routing, so they
  run on the spool consumer for spooled events.
  """

  import Ecto.Query

  alias Logflare.EnrichmentRules.Cache
  alias Logflare.EnrichmentRules.EnrichmentRule
  alias Logflare.LogEvent
  alias Logflare.Lql
  alias Logflare.Repo
  alias Logflare.SourceSchemas
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.RulesTree

  @type set_fields() :: [{[String.t()], term()}]
  @type compiled() :: %{tree: RulesTree.t(), set_fields: %{EnrichmentRule.id() => set_fields()}}

  @spec list_by_source_id(integer()) :: [EnrichmentRule.t()]
  def list_by_source_id(source_id) do
    from(r in EnrichmentRule, where: r.source_id == ^source_id, order_by: [asc: r.id])
    |> Repo.all()
  end

  @spec get(integer()) :: EnrichmentRule.t() | nil
  def get(id), do: Repo.get(EnrichmentRule, id)

  @doc """
  Parses the rule's LQL and inserts it. Takes string-keyed params.

  Values are typed from the source schema. A filter on a field the schema does not have yet, such
  as one added by key-value enrichment, is accepted with inferred types.
  """
  @spec create(Source.t(), map()) :: {:ok, EnrichmentRule.t()} | {:error, Ecto.Changeset.t()}
  def create(%Source{} = source, params) do
    params = Map.put(params, "source_id", source.id)

    changeset =
      case parse_lql(Map.get(params, "lql_string") || "", source) do
        {:ok, filters} ->
          EnrichmentRule.changeset(%EnrichmentRule{}, Map.put(params, "lql_filters", filters))

        {:error, message} ->
          %EnrichmentRule{}
          |> EnrichmentRule.changeset(params)
          |> Ecto.Changeset.add_error(:lql_string, message)
      end

    Repo.insert(changeset)
  end

  @spec delete(EnrichmentRule.t()) :: {:ok, EnrichmentRule.t()} | {:error, Ecto.Changeset.t()}
  def delete(%EnrichmentRule{} = rule), do: Repo.delete(rule)

  @doc """
  Builds the matching tree and parsed field paths for a source's rules. Returns nil when the
  source has no rules.
  """
  @spec compiled_by_source_id(integer()) :: compiled() | nil
  def compiled_by_source_id(source_id) do
    case list_by_source_id(source_id) do
      [] -> nil
      rules -> compile(rules)
    end
  end

  @spec compile([EnrichmentRule.t()]) :: compiled()
  def compile(rules) do
    %{
      tree: RulesTree.build(rules),
      set_fields: Map.new(rules, &{&1.id, parse_set_fields(&1.set_fields)})
    }
  end

  @doc """
  Sets the fields of every matching rule on each event. Rules are applied in creation order, so
  a later rule wins when two set the same field. Events routed into the source by another rule
  are left unchanged, and a list of only routed events skips the rules lookup.
  """
  @spec apply_rules([LogEvent.t()], Source.t()) :: [LogEvent.t()]
  def apply_rules([], _source), do: []

  def apply_rules(events, %Source{id: source_id}) do
    with true <- Enum.any?(events, &is_nil(&1.via_rule_id)),
         %{} = compiled <- Cache.compiled_by_source_id(source_id) do
      Enum.map(events, &apply_compiled(&1, compiled))
    else
      _ -> events
    end
  end

  @spec apply_compiled(LogEvent.t(), compiled()) :: LogEvent.t()
  defp apply_compiled(%LogEvent{via_rule_id: id} = event, _compiled) when id != nil, do: event

  defp apply_compiled(%LogEvent{} = event, %{tree: tree, set_fields: set_fields}) do
    case RulesTree.matching_rule_ids(event, tree) do
      [] ->
        event

      ids ->
        fields = ids |> Enum.sort() |> Enum.flat_map(&Map.get(set_fields, &1, []))
        LogEvent.put_fields(event, fields)
    end
  end

  @spec parse_set_fields(map()) :: set_fields()
  defp parse_set_fields(set_fields) do
    set_fields
    |> Enum.sort_by(fn {path, _value} -> path end)
    |> Enum.map(fn {path, value} -> {EnrichmentRule.parse_path(path), value} end)
  end

  @spec parse_lql(String.t(), Source.t()) :: {:ok, [term()]} | {:error, String.t()}
  defp parse_lql(lql_string, source) do
    case Lql.Parser.parse(lql_string, source_typemap(source)) do
      {:error, :field_not_found, _suggestion, _messages} ->
        lql_string |> Lql.Parser.parse() |> parse_result()

      result ->
        parse_result(result)
    end
  end

  defp parse_result({:ok, filters}), do: {:ok, filters}
  defp parse_result({:error, message}) when is_binary(message), do: {:error, message}
  defp parse_result({:error, message}), do: {:error, inspect(message)}

  @spec source_typemap(Source.t()) :: map()
  defp source_typemap(%Source{id: source_id}) do
    case SourceSchemas.Cache.get_source_schema_by(source_id: source_id) do
      %{bigquery_schema: %{} = schema} -> schema
      _ -> %{}
    end
  end
end
