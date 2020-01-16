defmodule Logflare.Rule do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Lql
  import Ecto.Changeset

  typed_schema "rules" do
    field :regex, :string
    field :regex_struct, Ecto.Regex
    field :sink, Ecto.UUID.Atom
    field :lql_filters, Ecto.Term, default: []
    field :lql_string, :string
    # TODO update sink field to be an belongs_to association
    # belongs_to :sink, Source, foreign_key: :sink_id, type: Ecto.UUID.Atom, references: :token
    belongs_to :source, Source

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:sink, :lql_string])
    |> parse_lql_string(rule.source_id)
    |> validate_required([:sink, :lql_filters, :lql_string])
    |> validate_regex_is_read_only()
  end

  def parse_lql_string(changeset, source_id) do
    source = Sources.get_by_and_preload(id: source_id)
      |> Sources.put_bq_table_data()

    lql_string = get_change(changeset, :lql_string)

    if lql_string do
      {:ok, lql_filters} = Lql.Parser.parse(lql_string, source.bq_table_schema)
      put_change(changeset, :lql_filters, lql_filters)
    else
      changeset
    end
  end

  def regex_to_lql_upgrade_changeset(rule) do
    if rule.regex do
      rule
      |> cast(
        %{lql_filters: [Lql.Utils.build_message_filter_rule_from_regex(rule.regex)]},
        [ :lql_filters ]
      )
    else
      cast(rule, %{}, [])
    end
  end

  def validate_regex_is_read_only(changeset) do
    validate_change(changeset, :regex, fn _field, regex ->
      if is_nil(regex) do
        []
      else
        [{"Regex source sink rules are read-only due to upgrade to LQL rules"}]
      end
    end)
  end

  def changeset_error_to_string(changeset) do
    changeset
    |> Ecto.Changeset.traverse_errors(fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(inspect(value)))
      end)
    end)
    |> Enum.reduce("", fn {k, v}, acc ->
      "#{acc}#{k}: #{v}\n"
    end)
  end
end
