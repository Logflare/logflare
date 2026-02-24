defmodule Logflare.Lql.Rules.FilterRule do
  @moduledoc """
  Represents a filter rule in LQL for searching and filtering log events.

  `FilterRule` supports filtering on event messages, metadata fields, timestamps, and nested
  field structures with various operators including exact matches, comparisons, regex,
  array operations, and range queries.

  ## Event Message Filtering Examples:
  - `error` - match exact string in event message
  - `"staging error"` - match exact string with spaces
  - `~server\\_\\d` - regex match in event message
  - `~"(?i)error"` - case insensitive regex match
  - `~"jpg$|jpeg$|png$"` - regex with OR patterns

  ## Metadata Field Filtering Examples:
  - `m.status:success` - exact match on metadata field
  - `m.response_code:>300` - numeric comparison operators (>, >=, <, <=)
  - `m.browser:~"Firefox 5\\d"` - regex match on metadata field
  - `m.user.roles:@>"admin"` - array includes value
  - `m.tags:@>~prod` - array includes regex match
  - `m.flag:true` - boolean value match
  - `m.optional_field:NULL` - NULL value match
  - `-m.environment:"production"` - negated filter (does NOT match)

  ## Timestamp Filtering Examples:
  - `t:today` - filter for today's events
  - `t:yesterday` - filter for yesterday's events
  - `t:last@5m` - events from last 5 minutes
  - `t:this@week` - events from current week
  - `t:>2023-01-01` - events after specific date
  - `t:2023-01-{01..05}` - date range using range syntax
  - `t:2023-01-01T10:{30..45}:00` - datetime range with time components

  ## Nested Field Support:
  - `m.user.profile.name:john` - deeply nested metadata field
  - `request.headers.user_agent:~Chrome` - nested request data
  - `config.database.pool_size:>10` - configuration field filtering

  ## Range Operators:
  - `m.latency:100.0..500.0` - numeric range for float values
  - `m.count:10..100` - numeric range for integer values
  - `m.level:debug..error` - level range (automatically expands to individual levels)

  ## Supported Operators:
  - `:=` - exact match (default when no operator specified)
  - `:>`, `:>=`, `:<`, `:<=` - comparison operators
  - `:~` - regex match
  - `:@>` - array includes value
  - `:@>~` - array includes regex match
  - `:..` - range operator (e.g., `10..100`)
  - `-` prefix - negation modifier

  ## Field Structure:
  - `path` - dot-separated field path (e.g., "metadata.user.profile.name")
  - `operator` - comparison operator atom (e.g., :=, :>, :~)
  - `value` - single value to match against
  - `values` - array of values for range operations
  - `modifiers` - additional options like negation, quoting, etc.
  - `shorthand` - original shorthand notation for timestamp filters
  """

  use TypedEctoSchema
  import Ecto.Changeset

  alias Ecto.Changeset

  @derive {Jason.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true
    field :operator, Ecto.Atom, virtual: true
    field :value, :any, virtual: true
    field :values, {:array, :any}, virtual: true
    field :modifiers, {:map, Ecto.Atom}, virtual: true, default: %{}
    field :shorthand, :string, virtual: true
  end

  @spec changeset(any(), __MODULE__.t()) :: Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, virtual_fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Changeset.t()
  def changeset(rule, params) do
    cast(rule, params, virtual_fields())
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
  Extracts timestamp filter rules from a list of `FilterRule` structs.

  Returns only `FilterRule` structs where the path is "timestamp".
  """
  @spec extract_timestamp_filters([__MODULE__.t()]) :: [__MODULE__.t()]
  def extract_timestamp_filters(filter_rules) when is_list(filter_rules) do
    Enum.filter(filter_rules, &(&1.path == "timestamp"))
  end

  @doc """
  Extracts non-timestamp filter rules from a list of FilterRule structs.

  Returns `FilterRule` structs where the path is NOT "timestamp" (metadata and message filters).
  """
  @spec extract_metadata_filters([__MODULE__.t()]) :: [__MODULE__.t()]
  def extract_metadata_filters(filter_rules) when is_list(filter_rules) do
    Enum.filter(filter_rules, &(&1.path != "timestamp"))
  end

  @doc """
  Checks if a `FilterRule` uses timestamp shorthand notation.

  Returns true for shorthand patterns like:
  - "last@5minute", "this@hour"
  - "today", "yesterday"

  Returns false for other patterns or nil shorthand.
  """
  @spec shorthand_timestamp?(__MODULE__.t()) :: boolean()
  def shorthand_timestamp?(%__MODULE__{shorthand: shorthand}) do
    case shorthand do
      x when is_binary(x) and binary_part(x, 0, min(byte_size(x), 4)) in ["last", "this"] -> true
      x when x in ["today", "yesterday"] -> true
      _ -> false
    end
  end

  @doc """
  Creates a new timestamp range `FilterRule` by jumping forward or backward in time.

  Takes existing timestamp filters, calculates the time difference, and creates
  a new range filter shifted by that interval in the specified direction.
  """
  @spec jump_timestamps([__MODULE__.t()], :backwards | :forwards) :: [__MODULE__.t()]
  def jump_timestamps(filter_rules, direction)
      when is_list(filter_rules) and direction in [:backwards, :forwards] do
    timestamp_rules = extract_timestamp_filters(filter_rules)

    timestamps =
      timestamp_rules
      |> Enum.map(&(&1.value || &1.values))
      |> List.flatten()
      |> Enum.reject(&is_nil/1)

    case timestamps do
      [] ->
        []

      _ ->
        from = Enum.min(timestamps)
        to = Enum.max(timestamps)

        diff =
          case direction do
            :forwards ->
              -NaiveDateTime.diff(from, to, :microsecond)

            :backwards ->
              NaiveDateTime.diff(from, to, :microsecond)
          end

        new_from = NaiveDateTime.add(from, diff, :microsecond)
        new_to = NaiveDateTime.add(to, diff, :microsecond)

        [
          build(
            modifiers: %{},
            operator: :range,
            path: "timestamp",
            shorthand: nil,
            value: nil,
            values: [new_from, new_to]
          )
        ]
    end
  end
end
