defmodule Logflare.Rule do
  @moduledoc false
  use Ecto.Schema
  alias Logflare.Source
  import Ecto.Changeset

  schema "rules" do
    field :regex, :string
    field :regex_struct, Ecto.Regex
    field :sink, Ecto.UUID.Atom
    # TODO update sink field to be an belongs_to association
    # belongs_to :sink, Source, foreign_key: :sink_id, type: Ecto.UUID.Atom, references: :token
    belongs_to :source, Source

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:regex, :sink])
    |> validate_required([:regex, :sink])
    |> validate_regex()
    |> cast(%{"regex_struct" => attrs["regex"]}, [:regex_struct])
    |> Map.update!(:errors, &Keyword.drop(&1, [:regex_struct]))
  end

  def validate_regex(changeset) do
    validate_change(changeset, :regex, fn field, regex ->
      case Regex.compile(regex) do
        {:ok, _} -> []
        {:error, {msg, position}} -> [{field, "#{msg} at position #{position}"}]
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
