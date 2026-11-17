defmodule Logflare.AlertIssues.NotificationPolicy do
  @moduledoc """
  Decides which alert issue changes are sent to the control plane, and for which
  receivers. Empty matchers match everything; `receivers` are opaque names the control
  plane maps to destinations.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @events ~w(opened reopened repeat resolved)
  @default_events ~w(opened reopened repeat)
  @categories ~w(security performance health)
  @severity_rank %{"info" => 1, "warning" => 2, "major" => 3, "critical" => 4}
  @rule_key_glob ~r/^[a-z0-9_.*\-]{1,128}$/
  @identifier_key ~r/^[a-z0-9_]{1,64}$/
  @receiver ~r/^[A-Za-z0-9_.:\-]{1,64}$/
  @min_repeat_interval 300

  @derive {Jason.Encoder,
           only: [
             :id,
             :name,
             :enabled,
             :rule_keys,
             :min_severity,
             :categories,
             :identifiers,
             :events,
             :repeat_interval_seconds,
             :receivers,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "alert_notification_policies" do
    field :name, :string
    field :enabled, :boolean, default: true
    field :rule_keys, {:array, :string}, default: []
    field :min_severity, Ecto.Enum, values: [:critical, :major, :warning, :info]
    field :categories, {:array, :string}, default: []
    field :identifiers, :map, default: %{}
    field :events, {:array, :string}, default: @default_events
    field :repeat_interval_seconds, :integer
    field :receivers, {:array, :string}, default: []

    belongs_to :user, Logflare.User

    timestamps(type: :utc_datetime_usec)
  end

  @spec events() :: [String.t()]
  def events, do: @events

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(policy, attrs) do
    policy
    |> cast(attrs, [
      :name,
      :enabled,
      :rule_keys,
      :min_severity,
      :categories,
      :identifiers,
      :events,
      :repeat_interval_seconds,
      :receivers
    ])
    |> validate_required([:name])
    |> validate_receivers_present()
    |> validate_length(:name, min: 1, max: 128)
    |> validate_length(:receivers, min: 1, max: 20)
    |> validate_length(:events, min: 1)
    |> validate_subset(:events, @events)
    |> validate_subset(:categories, @categories)
    |> validate_number(:repeat_interval_seconds, greater_than_or_equal_to: @min_repeat_interval)
    |> validate_each(:rule_keys, @rule_key_glob, "must be lowercase rule key globs")
    |> validate_each(:receivers, @receiver, "must be receiver names")
    |> validate_change(:identifiers, &validate_identifiers/2)
    |> unique_constraint(:name, name: :alert_notification_policies_user_id_name_index)
  end

  defp validate_receivers_present(changeset) do
    case get_field(changeset, :receivers) do
      [_ | _] -> changeset
      _ -> add_error(changeset, :receivers, "must name at least one receiver")
    end
  end

  defp validate_each(changeset, field, pattern, message) do
    validate_change(changeset, field, fn ^field, values ->
      if Enum.all?(values, &Regex.match?(pattern, &1)), do: [], else: [{field, message}]
    end)
  end

  defp validate_identifiers(:identifiers, identifiers) do
    valid? =
      Enum.all?(identifiers, fn {key, value} ->
        is_binary(key) and Regex.match?(@identifier_key, key) and is_binary(value)
      end)

    if valid?, do: [], else: [identifiers: "must map identifier keys to string values"]
  end

  @doc """
  Whether the policy wants `event` (opened, reopened, repeat or resolved) for an alert
  issue snapshot.
  """
  @spec matches?(t(), String.t(), map()) :: boolean()
  def matches?(%__MODULE__{enabled: false}, _event, _issue), do: false

  def matches?(%__MODULE__{} = policy, event, issue) do
    event in policy.events and rule_key_matches?(policy.rule_keys, issue["rule_key"]) and
      severity_matches?(policy.min_severity, issue["severity"]) and
      (policy.categories == [] or issue["category"] in policy.categories) and
      Enum.all?(policy.identifiers, fn {key, value} ->
        Map.get(issue["identifiers"] || %{}, key) == value
      end)
  end

  defp rule_key_matches?([], _rule_key), do: true
  defp rule_key_matches?(_globs, nil), do: false
  defp rule_key_matches?(globs, rule_key), do: Enum.any?(globs, &glob_match?(&1, rule_key))

  defp glob_match?(glob, value) do
    pattern =
      glob
      |> String.split("*")
      |> Enum.map_join(".*", &Regex.escape/1)

    Regex.match?(~r/\A#{pattern}\z/, value)
  end

  defp severity_matches?(nil, _severity), do: true

  defp severity_matches?(min, severity),
    do: Map.get(@severity_rank, severity, 0) >= Map.fetch!(@severity_rank, Atom.to_string(min))
end
