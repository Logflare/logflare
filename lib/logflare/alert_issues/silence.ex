defmodule Logflare.AlertIssues.Silence do
  @moduledoc """
  Hides alert issues whose rule key and identifiers match. A nil `rule_key` matches
  every rule; every `matchers` entry must equal the alert issue's identifier.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder,
           only: [:id, :rule_key, :matchers, :comment, :created_by, :inserted_at, :updated_at]}
  typed_schema "alert_issue_silences" do
    field :rule_key, :string
    field :matchers, :map, default: %{}
    field :comment, :string
    field :created_by, :string

    belongs_to :user, Logflare.User

    timestamps(type: :utc_datetime_usec)
  end

  @matcher_key ~r/^[a-z0-9_]{1,64}$/

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(silence, attrs) do
    silence
    |> cast(attrs, [:rule_key, :matchers, :comment, :created_by])
    |> validate_length(:rule_key, max: 128)
    |> validate_length(:comment, max: 1000)
    |> validate_change(:matchers, &validate_matchers/2)
  end

  defp validate_matchers(:matchers, matchers) do
    valid? =
      Enum.all?(matchers, fn {key, value} ->
        is_binary(key) and Regex.match?(@matcher_key, key) and is_binary(value)
      end)

    if valid?, do: [], else: [matchers: "must map identifier keys to string values"]
  end

  @spec matches?(t(), String.t() | nil, map()) :: boolean()
  def matches?(%__MODULE__{rule_key: rule_key}, issue_rule_key, _identifiers)
      when is_binary(rule_key) and rule_key != issue_rule_key,
      do: false

  def matches?(%__MODULE__{matchers: matchers}, _rule_key, identifiers) do
    identifiers = identifiers || %{}
    Enum.all?(matchers, fn {key, value} -> Map.get(identifiers, key) == value end)
  end
end
