defmodule Logflare.Lql.Rules.SelectRule do
  @moduledoc """
  Represents a field selection rule in LQL.

  Examples:
  - `s:*` - select all fields (wildcard)
  - `s:field` - select specific top-level field
  - `s:event_message` - select event message field
  - `s:m.user_id` - select metadata field (shorthand)
  - `select:metadata.user.id` - select nested metadata field (full syntax)
  - `s:user.profile.settings.theme` - select deeply nested field
  - `s:my_field.that.is.nested` - select arbitrary nested JSON/STRUCT field

  The `path` field contains the full dot-separated field path, supporting unlimited
  nesting depth for JSON objects and STRUCT types in backends like BigQuery.
  """

  use TypedEctoSchema
  import Ecto.Changeset

  alias Ecto.Changeset

  @derive {Jason.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true
    field :wildcard, :boolean, virtual: true, default: false
  end

  @spec changeset(any(), __MODULE__.t()) :: Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Changeset.t()
  def changeset(%__MODULE__{} = rule, params) do
    cast(rule, params, fields())
    |> validate_path()
  end

  @spec build(list()) :: __MODULE__.t()
  def build(params) when is_list(params) do
    %__MODULE__{}
    |> cast(Map.new(params), fields())
    |> validate_path()
    |> case do
      %{valid?: true} = changeset -> changeset.changes
      %{valid?: false} -> %{}
    end
  end

  @spec fields() :: list(atom())
  def fields() do
    __MODULE__.__schema__(:fields)
  end

  @spec build_from_path(String.t()) :: map()
  def build_from_path(path) when is_binary(path) do
    wildcard = path == "*"

    %__MODULE__{}
    |> cast(%{path: path, wildcard: wildcard}, fields())
    |> validate_path()
    |> case do
      %{valid?: true} = changeset ->
        Map.merge(%{path: path, wildcard: wildcard}, changeset.changes)

      %{valid?: false} ->
        %{path: path, wildcard: wildcard}
    end
  end

  @spec build_from_path(nil) :: map()
  def build_from_path(nil) do
    build_from_path("*")
  end

  # =============================================================================
  # Rule-Specific Operations
  # =============================================================================

  @doc """
  Normalizes a list of SelectRule structs by applying wildcard precedence and deduplication.

  ## Wildcard Precedence
  If any select rule uses wildcard (s:*), only the first wildcard rule is returned
  since wildcard selection supersedes all specific field selections.

  ## Deduplication
  When no wildcards are present, duplicate field paths are removed, keeping the
  first occurrence of each unique path.

  ## Examples

      # Wildcard wins
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "*", wildcard: true},
        %SelectRule{path: "field2", wildcard: false}
      ]
      SelectRule.normalize(rules)
      # => [%SelectRule{path: "*", wildcard: true}]

      # Deduplication without wildcards
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "field2", wildcard: false},
        %SelectRule{path: "field1", wildcard: false}  # duplicate
      ]
      SelectRule.normalize(rules)
      # => [%SelectRule{path: "field1", wildcard: false},
      #     %SelectRule{path: "field2", wildcard: false}]
  """
  @spec normalize([__MODULE__.t()]) :: [__MODULE__.t()]
  def normalize(select_rules) when is_list(select_rules) do
    select_rules
    |> apply_wildcard_precedence()
    |> deduplicate()
  end

  @doc """
  Applies wildcard precedence to a list of SelectRule structs.

  If any rule uses wildcard selection (path: "*"), returns only the first
  wildcard rule. Otherwise returns all rules unchanged.
  """
  @spec apply_wildcard_precedence([__MODULE__.t()]) :: [__MODULE__.t()]
  def apply_wildcard_precedence(select_rules) when is_list(select_rules) do
    case Enum.any?(select_rules, & &1.wildcard) do
      true ->
        # Wildcard wins - return only the first wildcard rule
        select_rules
        |> Enum.find(& &1.wildcard)
        |> List.wrap()

      false ->
        select_rules
    end
  end

  @doc """
  Removes duplicate field selections from a list of SelectRule structs.

  Keeps the first occurrence of each unique field path.
  """
  @spec deduplicate([__MODULE__.t()]) :: [__MODULE__.t()]
  def deduplicate(select_rules) when is_list(select_rules) do
    select_rules
    |> Enum.uniq_by(& &1.path)
  end

  # =============================================================================
  # Private Functions
  # =============================================================================

  defp validate_path(%Changeset{} = changeset) do
    changeset
    |> validate_required([:path])
    |> validate_change(:path, fn :path, path ->
      cond do
        path == "*" ->
          []

        is_binary(path) and
            String.match?(path, ~r/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/) ->
          []

        true ->
          [path: "must be '*' for wildcard or a valid dot-separated field path"]
      end
    end)
    |> put_change(:wildcard, get_field(changeset, :path) == "*")
  end
end
