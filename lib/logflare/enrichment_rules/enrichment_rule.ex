defmodule Logflare.EnrichmentRules.EnrichmentRule do
  @moduledoc """
  Conditions on a source's events and the fields set on each event that meets them.

  `match` is `:all` or `:any` of `conditions`. Each condition is a map with a `"path"`, a
  `"predicate"`, a `"value"` for comparing predicates, and an optional `"case_insensitive"`.
  `set_fields` maps a field path to a scalar value. Paths use dot syntax, with `m.` as shorthand
  for `metadata.`. A condition path of `@severity_text` reads the severity that the ClickHouse log
  mapping derives from the event.
  """
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Sources.Source

  @reserved_heads ~w(id event_message timestamp)
  @value_predicates ~w(equals not_equals)
  @numeric_predicates ~w(greater_than less_than)
  @list_predicates ~w(in not_in)
  @presence_predicates ~w(exists not_exists is_empty not_empty)
  @predicates @value_predicates ++ @numeric_predicates ++ @list_predicates ++ @presence_predicates
  @derived_fields ~w(severity_text)

  @derive {Jason.Encoder, only: [:id, :token, :source_id, :match, :conditions, :set_fields]}

  @type id() :: non_neg_integer()
  @type condition() :: %{required(String.t()) => term()}

  typed_schema "enrichment_rules" do
    field :token, Ecto.UUID, autogenerate: true
    field :match, Ecto.Enum, values: [:all, :any], default: :all
    field :conditions, {:array, :map}
    field :set_fields, :map

    belongs_to :source, Source

    timestamps()
  end

  @spec predicates() :: [String.t()]
  def predicates, do: @predicates

  @doc """
  Returns the derived field a condition path names (`@severity_text`), or nil for an event path.
  """
  @spec derived_field(String.t()) :: String.t() | nil
  def derived_field("@" <> name), do: name
  def derived_field(_path), do: nil

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(rule, attrs) do
    rule
    |> cast(attrs, [:match, :conditions, :set_fields, :source_id])
    |> validate_required([:match, :conditions, :set_fields, :source_id])
    |> validate_length(:conditions, min: 1, message: "must contain at least one condition")
    |> validate_change(:conditions, &validate_conditions/2)
    |> validate_change(:set_fields, &validate_set_fields/2)
    |> foreign_key_constraint(:source_id)
  end

  @doc """
  Splits a field path into its segments, expanding the `m.` prefix.
  """
  @spec parse_path(String.t()) :: [String.t()]
  def parse_path(path) do
    path
    |> String.trim()
    |> String.replace_prefix("m.", "metadata.")
    |> String.split(".")
  end

  defp validate_conditions(field, conditions) do
    Enum.flat_map(conditions, &condition_errors(field, &1))
  end

  defp condition_errors(field, %{"path" => path, "predicate" => predicate} = condition)
       when is_binary(path) and is_binary(predicate) do
    cond do
      not valid_condition_path?(path) ->
        [{field, "has an invalid field path: #{inspect(path)}"}]

      predicate not in @predicates ->
        [{field, "has an unknown predicate: #{predicate}"}]

      not is_boolean(Map.get(condition, "case_insensitive", false)) ->
        [{field, "#{path} case_insensitive must be true or false"}]

      true ->
        value_errors(field, path, predicate, Map.get(condition, "value"))
    end
  end

  defp condition_errors(field, condition),
    do: [{field, "must have a path and a predicate: #{inspect(condition)}"}]

  defp value_errors(field, path, predicate, value) when predicate in @value_predicates do
    if scalar?(value),
      do: [],
      else: [{field, "#{path} #{predicate} needs a string, number or boolean value"}]
  end

  defp value_errors(field, path, predicate, value) when predicate in @numeric_predicates do
    if is_number(value), do: [], else: [{field, "#{path} #{predicate} needs a number"}]
  end

  defp value_errors(field, path, predicate, [_ | _] = values)
       when predicate in @list_predicates do
    if Enum.all?(values, &scalar?/1),
      do: [],
      else: [{field, "#{path} #{predicate} values must be strings, numbers or booleans"}]
  end

  defp value_errors(field, path, predicate, _value) when predicate in @list_predicates,
    do: [{field, "#{path} #{predicate} needs a list of values"}]

  defp value_errors(_field, _path, _predicate, _value), do: []

  defp validate_set_fields(field, set_fields) when map_size(set_fields) == 0,
    do: [{field, "must set at least one field"}]

  defp validate_set_fields(field, set_fields) when is_map(set_fields) do
    Enum.flat_map(set_fields, fn {path, value} -> set_field_errors(field, path, value) end)
  end

  defp validate_set_fields(field, _set_fields),
    do: [{field, "must be a map of field paths to values"}]

  defp set_field_errors(field, path, value) when is_binary(path) do
    cond do
      not valid_path?(path) ->
        [{field, "has an invalid field path: #{inspect(path)}"}]

      hd(parse_path(path)) in @reserved_heads ->
        [{field, "cannot set reserved field: #{path}"}]

      not scalar?(value) ->
        [{field, "value for #{path} must be a string, number or boolean"}]

      true ->
        []
    end
  end

  defp set_field_errors(field, path, _value),
    do: [{field, "has an invalid field path: #{inspect(path)}"}]

  defp valid_condition_path?(path) do
    case derived_field(path) do
      nil -> valid_path?(path)
      name -> name in @derived_fields
    end
  end

  defp valid_path?(path) when is_binary(path) do
    path |> parse_path() |> Enum.all?(&(&1 != "" and not String.contains?(&1, ["[", "]"])))
  end

  defp scalar?(value), do: is_binary(value) or is_number(value) or is_boolean(value)
end
