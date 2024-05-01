defmodule Logflare.Rule do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Source
  alias Logflare.Backends.Backend
  alias Logflare.Lql.Parser
  import Ecto.Changeset

  typed_schema "rules" do
    field :sink, Ecto.UUID.Atom
    field :lql_filters, Ecto.Term, default: []
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
    qs = get_field(changeset, :lql_string)

    case Parser.parse(qs) do
      {:ok, rules} -> put_change(changeset, :lql_filters, rules)
      _ -> changeset
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
