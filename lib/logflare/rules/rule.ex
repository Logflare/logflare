defmodule Logflare.Rules.Rule do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Source
  alias Logflare.Backends.Backend
  alias Logflare.Lql.Parser
  import Ecto.Changeset

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
