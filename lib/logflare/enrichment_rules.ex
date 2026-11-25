defmodule Logflare.EnrichmentRules do
  @moduledoc """
  Conditional enrichment rules. Each rule is a set of conditions on a source's events and the
  fields it sets on every event that meets them. Rules are applied when events are dispatched,
  before routing, so they run on the spool consumer for spooled events.

  A source's rules compile into one `Logflare.Mapper` mapping with an `enum8` field per rule,
  which reads only the paths the conditions name.
  """

  import Ecto.Query

  alias Logflare.EnrichmentRules.Cache
  alias Logflare.EnrichmentRules.EnrichmentRule
  alias Logflare.LogEvent
  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig
  alias Logflare.Mapper.MappingConfig.InferCondition
  alias Logflare.Mapper.MappingConfig.InferRule
  alias Logflare.Repo
  alias Logflare.Sources.Source

  @unmatched "1"
  @matched 2

  @type set_fields() :: [{[String.t()], term()}]
  @type compiled() :: %{
          mapping: reference(),
          fields: [{String.t(), EnrichmentRule.id()}],
          set_fields: %{EnrichmentRule.id() => set_fields()}
        }

  @spec list_by_source_id(integer()) :: [EnrichmentRule.t()]
  def list_by_source_id(source_id) do
    from(r in EnrichmentRule, where: r.source_id == ^source_id, order_by: [asc: r.id])
    |> Repo.all()
  end

  @spec get(integer()) :: EnrichmentRule.t() | nil
  def get(id), do: Repo.get(EnrichmentRule, id)

  @doc """
  Validates the rule, checks its conditions compile, and inserts it. Takes string-keyed params.
  """
  @spec create(Source.t(), map()) :: {:ok, EnrichmentRule.t()} | {:error, Ecto.Changeset.t()}
  def create(%Source{} = source, params) do
    %EnrichmentRule{}
    |> EnrichmentRule.changeset(Map.put(params, "source_id", source.id))
    |> validate_compiles()
    |> Repo.insert()
  end

  @spec delete(EnrichmentRule.t()) :: {:ok, EnrichmentRule.t()} | {:error, Ecto.Changeset.t()}
  def delete(%EnrichmentRule{} = rule), do: Repo.delete(rule)

  @doc """
  Compiles a source's rules into a mapping and parsed field paths. Returns nil when the source
  has no rules.
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
      mapping: rules |> mapping_config() |> Mapper.compile!(),
      fields: Enum.map(rules, &{field_name(&1), &1.id}),
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

  defp apply_compiled(%LogEvent{body: body} = event, compiled) do
    %{mapping: mapping, fields: fields, set_fields: set_fields} = compiled
    output = Mapper.map(body, mapping)

    case for {name, id} <- fields, Map.get(output, name) == @matched, do: id do
      [] -> event
      ids -> LogEvent.put_fields(event, Enum.flat_map(ids, &Map.fetch!(set_fields, &1)))
    end
  end

  @spec validate_compiles(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  defp validate_compiles(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  defp validate_compiles(changeset) do
    rule = Ecto.Changeset.apply_changes(changeset)

    case rule |> List.wrap() |> mapping_config() |> Mapper.compile() do
      {:ok, _mapping} -> changeset
      {:error, reason} -> Ecto.Changeset.add_error(changeset, :conditions, reason)
    end
  end

  @spec mapping_config([EnrichmentRule.t()]) :: MappingConfig.t()
  defp mapping_config(rules), do: rules |> Enum.map(&rule_field/1) |> MappingConfig.new()

  @spec rule_field(EnrichmentRule.t()) :: FieldConfig.t()
  defp rule_field(%EnrichmentRule{match: match, conditions: conditions} = rule) do
    infer = %InferRule{result: "matched"} |> Map.put(match, Enum.map(conditions, &to_condition/1))

    FieldConfig.enum8(field_name(rule),
      values: %{"unmatched" => 1, "matched" => @matched},
      default: @unmatched,
      infer: [infer]
    )
  end

  @spec to_condition(EnrichmentRule.condition()) :: InferCondition.t()
  defp to_condition(%{"path" => path, "predicate" => "in"} = condition) do
    %InferCondition{
      path: mapper_path(path),
      predicate: "in",
      comparison_values: condition["value"]
    }
  end

  defp to_condition(%{"path" => path, "predicate" => predicate} = condition) do
    %InferCondition{
      path: mapper_path(path),
      predicate: predicate,
      comparison_value: condition["value"]
    }
  end

  @spec mapper_path(String.t()) :: String.t()
  defp mapper_path(path), do: "$." <> Enum.join(EnrichmentRule.parse_path(path), ".")

  @spec field_name(EnrichmentRule.t()) :: String.t()
  defp field_name(%EnrichmentRule{id: id}), do: "rule_#{id}"

  @spec parse_set_fields(map()) :: set_fields()
  defp parse_set_fields(set_fields) do
    set_fields
    |> Enum.sort_by(fn {path, _value} -> path end)
    |> Enum.map(fn {path, value} -> {EnrichmentRule.parse_path(path), value} end)
  end
end
