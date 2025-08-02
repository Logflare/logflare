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
  import Logflare.Utils.Guards

  alias Ecto.Changeset

  @derive {Jason.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true
    field :wildcard, :boolean, virtual: true, default: false
  end

  @spec changeset(any(), __MODULE__.t()) :: Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, virtual_fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Changeset.t()
  def changeset(%__MODULE__{} = rule, params) do
    cast(rule, params, virtual_fields())
    |> validate_path()
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

  @spec build_from_path(String.t()) :: __MODULE__.t()
  def build_from_path(path) when is_non_empty_binary(path) do
    build(path: path)
  end

  @spec build_from_path(any()) :: __MODULE__.t()
  def build_from_path(_path), do: build_from_path("*")

  @spec virtual_fields() :: list(atom())
  def virtual_fields() do
    __MODULE__.__schema__(:virtual_fields)
  end

  # =============================================================================
  # Rule-Specific Operations
  # =============================================================================

  @doc """
  Normalizes a list of `SelectRule` structs by applying wildcard precedence and path deduplication.

  ## Wildcard Precedence
  If any select rule uses wildcard `s:*`, only the first wildcard rule is returned
  since wildcard selection supersedes all specific field selections.
  """
  @spec normalize([__MODULE__.t()]) :: [__MODULE__.t()]
  def normalize(select_rules) when is_list(select_rules) do
    select_rules
    |> apply_wildcard_precedence()
    |> deduplicate_paths()
  end

  @doc """
  Applies wildcard precedence to a list of `SelectRule` structs.
  """
  @spec apply_wildcard_precedence([__MODULE__.t()]) :: [__MODULE__.t()]
  def apply_wildcard_precedence(select_rules) when is_list(select_rules) do
    case Enum.any?(select_rules, & &1.wildcard) do
      true ->
        select_rules
        |> Enum.find(& &1.wildcard)
        |> List.wrap()

      false ->
        select_rules
    end
  end

  @doc """
  Removes duplicate field selections from a list of `SelectRule` structs based on the path attribute.
  """
  @spec deduplicate_paths([__MODULE__.t()]) :: [__MODULE__.t()]
  def deduplicate_paths(select_rules) when is_list(select_rules) do
    Enum.uniq_by(select_rules, & &1.path)
  end

  # =============================================================================
  # Private Functions
  # =============================================================================

  defp validate_path(%Changeset{} = changeset) do
    updated_changeset =
      changeset
      |> validate_required([:path])
      |> validate_change(:path, fn :path, path ->
        cond do
          path == "*" ->
            []

          is_non_empty_binary(path) and
              String.match?(path, ~r/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/) ->
            []

          true ->
            [path: "must be '*' for wildcard or a valid dot-separated field path"]
        end
      end)

    path = get_field(updated_changeset, :path)
    force_change(updated_changeset, :wildcard, path == "*")
  end
end
