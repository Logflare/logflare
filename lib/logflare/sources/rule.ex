defmodule Logflare.Rule do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Source
  alias Logflare.Lql
  import Ecto.Changeset

  typed_schema "rules" do
    field :regex, :string
    field :regex_struct, Ecto.Regex
    field :sink, Ecto.UUID.Atom
    embeds_many :lql_filters, Lql.FilterRule
    # TODO update sink field to be an belongs_to association
    # belongs_to :sink, Source, foreign_key: :sink_id, type: Ecto.UUID.Atom, references: :token
    belongs_to :source, Source

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:sink, :lql_filters])
    |> validate_required([:sink, :lql_filters])
    |> validate_regex_is_read_only()
  end

  def regex_to_lql_upgrade_changeset(rule) do
    if rule.regex do
      rule
      |> cast(
        %{lql_filters: [Lql.Utils.build_message_filter_rule_from_regex(rule.regex)]},
        [
          :lql_filters
        ]
      )
    else
      rule
      |> cast(%{}, [])
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
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
    |> Enum.reduce("", fn {k, v}, acc ->
      "#{acc}#{k}: #{v}\n"
    end)
  end
end
