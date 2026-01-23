defmodule Logflare.Rules.Rule do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Sources.Source
  alias Logflare.Backends.Backend
  alias Logflare.Lql.Parser

  @type id() :: non_neg_integer()

  @derive {Jason.Encoder,
           only: [
             :token,
             :id,
             :source_id,
             :lql_string,
             :backend_id
           ]}

  typed_schema "rules" do
    field :sink, Ecto.UUID.Atom
    field :token, Ecto.UUID, autogenerate: true
    field :lql_filters, Ecto.LqlRules, default: []
    field :lql_string, :string
    belongs_to :source, Source
    belongs_to :backend, Backend

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:sink, :lql_string, :lql_filters, :backend_id, :source_id])
    |> validate_required([:lql_string])
    |> maybe_parse_lql()
    |> validate_length(:lql_filters, min: 1)
    |> foreign_key_constraint(:source_id)
  end

  defp maybe_parse_lql(changeset) do
    {:ok, rules} =
      changeset
      |> get_field(:lql_string)
      |> Parser.parse()

    put_change(changeset, :lql_filters, rules)
  end
end
