defmodule Logflare.Rule do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Source
  import Ecto.Changeset

  typed_schema "rules" do

    field :sink, Ecto.UUID.Atom
    field :lql_filters, Ecto.Term, default: []
    field :lql_string, :string
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
