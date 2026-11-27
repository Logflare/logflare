defmodule Logflare.EnrichmentRules do
  @moduledoc """
  Conditional enrichment rules. Each rule is a set of conditions on a source's events and the
  fields it sets on every event that meets them. Rules are applied when events are dispatched,
  before routing, so they run on the spool consumer for spooled events.

  A source's rules compile into one `Logflare.Mapper` mapping with an `enum8` field per rule,
  which reads only the paths the conditions name. Numeric comparisons accept strings holding a
  number. A condition on `@severity_text` reads the `severity_text` field of the ClickHouse log
  mapping, which is added to the mapping ahead of the rules.
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
  alias Logflare.Mapper.OtelDefaults
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

  @spec list_by_source_ids([integer()]) :: %{integer() => [EnrichmentRule.t()]}
  def list_by_source_ids(source_ids) do
    from(r in EnrichmentRule, where: r.source_id in ^source_ids, order_by: [asc: r.id])
    |> Repo.all()
    |> Enum.group_by(& &1.source_id)
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

  @doc """
  Replaces all of a source's rules with `rules_params`, in order, in one transaction.
  """
  @spec replace_for_source(Source.t(), [map()]) ::
          {:ok, [EnrichmentRule.t()]} | {:error, Ecto.Changeset.t()}
  def replace_for_source(%Source{} = source, rules_params) when is_list(rules_params) do
    Repo.transaction(fn ->
      Repo.delete_all(from(r in EnrichmentRule, where: r.source_id == ^source.id))

      Enum.map(rules_params, &create_or_rollback(source, &1))
    end)
  end

  def replace_for_source(%Source{}, _rules_params), do: {:error, not_a_list_changeset()}

  defp create_or_rollback(source, params) do
    case create(source, params) do
      {:ok, rule} -> rule
      {:error, changeset} -> Repo.rollback(changeset)
    end
  end

  @doc """
  Checks rule params without a source, as `replace_for_source/2` would.
  """
  @spec validate_params(term()) :: :ok | {:error, Ecto.Changeset.t()}
  def validate_params(rules_params) when is_list(rules_params) do
    Enum.find_value(rules_params, :ok, fn params ->
      changeset =
        %EnrichmentRule{}
        |> EnrichmentRule.changeset(Map.put(params, "source_id", 0))
        |> validate_compiles()

      if changeset.valid?, do: nil, else: {:error, changeset}
    end)
  end

  def validate_params(_rules_params), do: {:error, not_a_list_changeset()}

  @doc """
  The API representation of a rule.
  """
  @spec to_api(EnrichmentRule.t()) :: map()
  def to_api(%EnrichmentRule{} = rule) do
    %{
      "match" => Atom.to_string(rule.match),
      "conditions" => rule.conditions,
      "set_fields" => rule.set_fields
    }
  end

  defp not_a_list_changeset do
    {%{}, %{enrichment_rules: {:array, :map}}}
    |> Ecto.Changeset.change()
    |> Ecto.Changeset.add_error(:enrichment_rules, "must be a list of rules")
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
  defp mapping_config(rules) do
    MappingConfig.new(derived_fields(rules) ++ Enum.map(rules, &rule_field/1))
  end

  @spec derived_fields([EnrichmentRule.t()]) :: [FieldConfig.t()]
  defp derived_fields(rules) do
    names =
      for %EnrichmentRule{conditions: conditions} <- rules,
          %{"path" => path} <- conditions,
          name = EnrichmentRule.derived_field(path),
          name != nil,
          uniq: true,
          do: name

    case names do
      [] -> []
      names -> Enum.filter(OtelDefaults.for_log().fields, &(&1.name in names))
    end
  end

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
  defp to_condition(%{"path" => path, "predicate" => predicate} = condition) do
    value_key = if predicate in ~w(in not_in), do: :comparison_values, else: :comparison_value

    %InferCondition{
      predicate: predicate,
      case_insensitive: Map.get(condition, "case_insensitive", false),
      coercion: "lenient"
    }
    |> put_source(path)
    |> Map.put(value_key, condition["value"])
  end

  @spec put_source(InferCondition.t(), String.t()) :: InferCondition.t()
  defp put_source(condition, path) do
    case EnrichmentRule.derived_field(path) do
      nil -> %{condition | path: mapper_path(path)}
      name -> %{condition | from_output: name}
    end
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
