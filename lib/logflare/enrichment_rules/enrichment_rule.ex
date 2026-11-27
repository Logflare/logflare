defmodule Logflare.EnrichmentRules.EnrichmentRule do
  @moduledoc """
  An LQL filter on a source and the fields set on each event that matches it.

  `set_fields` maps a field path to a scalar value. Paths use dot syntax, with `m.` as
  shorthand for `metadata.`.
  """
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Sources.Source

  @reserved_heads ~w(id event_message timestamp)

  @derive {Jason.Encoder, only: [:id, :token, :source_id, :lql_string, :set_fields]}

  @type id() :: non_neg_integer()

  typed_schema "enrichment_rules" do
    field :token, Ecto.UUID, autogenerate: true
    field :lql_string, :string
    field :lql_filters, Ecto.LqlRules, default: []
    field :set_fields, :map

    belongs_to :source, Source

    timestamps()
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(rule, attrs) do
    rule
    |> cast(attrs, [:lql_string, :lql_filters, :set_fields, :source_id])
    |> validate_required([:lql_string, :set_fields, :source_id])
    |> validate_length(:lql_filters, min: 1, message: "must contain at least one filter")
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

  defp validate_set_fields(field, set_fields) when map_size(set_fields) == 0,
    do: [{field, "must set at least one field"}]

  defp validate_set_fields(field, set_fields) when is_map(set_fields) do
    Enum.flat_map(set_fields, fn {path, value} -> set_field_errors(field, path, value) end)
  end

  defp validate_set_fields(field, _set_fields),
    do: [{field, "must be a map of field paths to values"}]

  defp set_field_errors(field, path, value) when is_binary(path) do
    segments = parse_path(path)

    cond do
      Enum.any?(segments, &(&1 == "")) ->
        [{field, "has an invalid field path: #{inspect(path)}"}]

      hd(segments) in @reserved_heads ->
        [{field, "cannot set reserved field: #{path}"}]

      not scalar?(value) ->
        [{field, "value for #{path} must be a string, number or boolean"}]

      true ->
        []
    end
  end

  defp set_field_errors(field, path, _value),
    do: [{field, "has an invalid field path: #{inspect(path)}"}]

  defp scalar?(value), do: is_binary(value) or is_number(value) or is_boolean(value)
end
