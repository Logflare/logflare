defmodule Logflare.Lql.Rules.FromRule do
  @moduledoc """
  Represents a FROM clause in LQL, specifying the cte or source to query from.

  The `FromRule` allows explicit specification of which data source to query,
  enabling users to select between multiple CTEs in sandboxed queries or to
  explicitly reference sources in standard queries.

  ## Syntax Examples:
  - `f:my_table` - query from table "my_table"
  - `from:my_table` - alternative syntax for the same query
  - `f:errors m.level:critical` - query from "errors" table with filter
  - `f:aggregated_logs c:count(*)` - query from "aggregated_logs" with chart

  ## Sandboxed Query Context (CTE):
  In sandboxed endpoints with multiple CTEs, the from rule selects which CTE to query:
  ```
  f:error_logs c:count(*) c:group_by(t::hour)
  ```
  This queries from the `error_logs` CTE instead of the default (last) CTE.

  ## Standard Query Context (Source):
  In the search UI, the from rule references a specific source:
  ```
  f:my_source_name m.status:>399
  ```
  This explicitly queries from source "my_source_name".

  ## Validation:
  - Only one from rule is allowed per query
  - Table names must be valid identifiers (alphanumeric + underscore)
  - In sandboxed context, table must reference an available CTE
  - In standard context, table must reference an accessible source

  ## Field Structure:
  - `table` - table/CTE name or source identifier (required)
  - `table_type` - type of table reference (:cte, :source, or :unknown)

  ## Default Behavior:
  When no from rule is specified:
  - Sandboxed queries: Uses the LAST CTE (most refined/final dataset)
  - Standard queries: Uses the current source context
  """

  use TypedEctoSchema

  import Ecto.Changeset
  import Logflare.Utils.Guards

  alias Ecto.Changeset

  @derive {Jason.Encoder, []}

  @type table_type :: :cte | :source | :unknown

  @primary_key false
  typed_embedded_schema do
    field :table, :string, virtual: true

    field :table_type, Ecto.Enum,
      virtual: true,
      values: [:cte, :source, :unknown],
      default: :unknown
  end

  @spec changeset(any(), __MODULE__.t()) :: Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, virtual_fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Changeset.t()
  def changeset(%__MODULE__{} = rule, params) do
    rule
    |> cast(params, virtual_fields())
    |> validate_required([:table])
    |> validate_length(:table, min: 1, max: 255)
    |> validate_format(:table, ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/,
      message: "must be a valid identifier"
    )
  end

  @spec build(list()) :: __MODULE__.t()
  def build(params) when is_list(params) do
    changeset = changeset(%__MODULE__{}, Map.new(params))

    case changeset do
      %{valid?: true} ->
        Changeset.apply_changes(changeset)

      %{valid?: false} ->
        %__MODULE__{}
    end
  end

  @spec virtual_fields() :: list(atom())
  def virtual_fields do
    __MODULE__.__schema__(:virtual_fields)
  end

  # =============================================================================
  # Rule-Specific Operations
  # =============================================================================

  @doc """
  Creates a new `FromRule` with the given table name and optional table type.

  ## Examples

      iex> FromRule.new("my_table")
      %FromRule{table: "my_table", table_type: :unknown}

      iex> FromRule.new("errors_cte", :cte)
      %FromRule{table: "errors_cte", table_type: :cte}

  """
  @spec new(String.t(), table_type()) :: __MODULE__.t()
  def new(table, table_type \\ :unknown) when is_non_empty_binary(table) do
    %__MODULE__{table: table, table_type: table_type}
  end

  @doc """
  Gets the table name from a `FromRule`.
  """
  @spec get_table(__MODULE__.t()) :: String.t() | nil
  def get_table(%__MODULE__{table: table}), do: table

  @doc """
  Gets the table type from a `FromRule`.
  """
  @spec get_table_type(__MODULE__.t()) :: table_type()
  def get_table_type(%__MODULE__{table_type: table_type}), do: table_type
end
