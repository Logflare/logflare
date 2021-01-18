defmodule Logflare.Rule do
  @moduledoc false
  use TypedEctoSchema
  use Logflare.Commons
  import Ecto.Changeset
  use Logflare.ChangefeedSchema

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
    |> cast(attrs, [:sink, :lql_string, :lql_filters])
    |> validate_required([:sink, :lql_filters, :lql_string])
    |> validate_length(:lql_filters, min: 1)
    |> foreign_key_constraint(:source_id)
  end

  def regex_changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:sink, :regex, :regex_struct])
    |> validate_required([:sink, :regex, :regex_struct])
    |> foreign_key_constraint(:source_id)
  end

  @deprecated "Delete when all source rules are upgraded to LQL"
  @spec regex_to_lql_upgrade_changeset(Rule.t()) :: Ecto.Changeset.t()
  def regex_to_lql_upgrade_changeset(rule) do
    if rule.regex do
      lql_string = ~s|"#{rule.regex}"|
      {:ok, lql_filters} = Lql.build_message_filter_from_regex(lql_string)

      rule
      |> cast(
        %{
          lql_filters: lql_filters,
          lql_string: lql_string
        },
        [:lql_filters, :lql_string]
      )
    else
      cast(rule, %{}, [])
    end
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
