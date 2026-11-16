defmodule Logflare.AlertIssues.IssueTypes do
  @moduledoc """
  Alert issue types are a user's alert queries with a `rule_key`. Each type carries
  whether it is silenced for a given set of identifiers.
  """

  import Ecto.Query

  alias Ecto.Changeset
  alias Logflare.AlertIssues.LogEvents
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Repo
  alias Logflare.User

  @group_path ~r/^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$/
  @max_group_by 3
  @custom_cron "* * * * *"

  @type issue_type :: %{
          rule_key: String.t(),
          name: String.t(),
          description: String.t() | nil,
          category: atom(),
          severity: atom(),
          managed: boolean(),
          language: atom(),
          lql: String.t() | nil,
          group_by: [String.t()],
          silenced: boolean(),
          silence_id: integer() | nil
        }

  @spec list(User.t(), map()) :: [issue_type()]
  def list(%User{id: user_id} = user, identifiers) when is_map(identifiers) do
    silences = Silences.list(user)

    AlertQuery
    |> where([a], a.user_id == ^user_id and not is_nil(a.rule_key))
    |> order_by([a], asc: a.category, asc: a.name)
    |> Repo.all()
    |> Enum.map(&to_issue_type(&1, silences, identifiers))
  end

  @spec to_issue_type(AlertQuery.t(), [Logflare.AlertIssues.Silence.t()], map()) :: issue_type()
  def to_issue_type(%AlertQuery{} = alert, silences, identifiers) do
    silence = Silences.find_exact(silences, alert.rule_key, identifiers)

    %{
      rule_key: alert.rule_key,
      name: alert.name,
      description: alert.description,
      category: alert.category,
      severity: alert.severity,
      managed: alert.managed,
      language: alert.language,
      lql: if(alert.language == :lql, do: alert.query),
      group_by: alert.group_by || [],
      silenced: not is_nil(silence),
      silence_id: silence && silence.id
    }
  end

  @doc """
  Creates a custom alert issue type: a scheduled LQL rule evaluated every minute.
  Its log preview narrows the rule's LQL by each alert issue's group values.
  """
  @spec create_custom(User.t(), map()) :: {:ok, issue_type()} | {:error, Changeset.t()}
  def create_custom(%User{} = user, params) when is_map(params) do
    lql = Map.get(params, "lql")
    group_by = Map.get(params, "group_by") || []
    identifiers = Map.get(params, "identifiers") || %{}

    attrs = %{
      "name" => Map.get(params, "name"),
      "description" => Map.get(params, "description"),
      "language" => "lql",
      "query" => lql,
      "cron" => @custom_cron,
      "kind" => "scheduled",
      "managed" => false,
      "severity" => Map.get(params, "severity", "warning"),
      "category" => Map.get(params, "category", "health"),
      "group_by" => group_by,
      "scope" => identifiers,
      "rule_key" => unique_rule_key(user, Map.get(params, "name")),
      "signal_mapping" =>
        %{"preview" => %{"lql" => preview_lql(lql, group_by)}}
        |> put_notebook(Map.get(params, "notebook"))
    }

    changeset =
      user
      |> Ecto.build_assoc(:alert_queries)
      |> Map.put(:user, user)
      |> AlertQuery.changeset(attrs)
      |> validate_custom(user, lql, group_by, identifiers)

    with {:ok, _validated} <- Changeset.apply_action(changeset, :insert),
         {:ok, alert} <- Alerting.create_alert_query(user, attrs) do
      Alerting.schedule_alert(alert)
      {:ok, to_issue_type(alert, [], identifiers)}
    end
  end

  defp put_notebook(signal_mapping, nil), do: signal_mapping
  defp put_notebook(signal_mapping, notebook), do: Map.put(signal_mapping, "notebook", notebook)

  @spec delete_custom(User.t(), String.t()) :: :ok | {:error, :not_found | :managed}
  def delete_custom(%User{id: user_id}, rule_key) when is_binary(rule_key) do
    case Alerting.get_alert_query_by(user_id: user_id, rule_key: rule_key) do
      nil ->
        {:error, :not_found}

      %AlertQuery{managed: true} ->
        {:error, :managed}

      alert ->
        {:ok, _} = Alerting.delete_alert_query(alert)
        :ok
    end
  end

  defp validate_custom(changeset, user, lql, group_by, identifiers) do
    changeset
    |> Changeset.validate_required([:name, :query])
    |> Changeset.validate_length(:name, max: 100)
    |> validate_lql(user, lql)
    |> validate_group_by(group_by)
    |> validate_identifiers(identifiers)
  end

  defp validate_lql(changeset, user, lql) when is_binary(lql) do
    case LogEvents.validate(user, lql) do
      :ok -> changeset
      {:error, reason} -> Changeset.add_error(changeset, :lql, lql_error(reason))
    end
  end

  defp validate_lql(changeset, _user, _lql),
    do: Changeset.add_error(changeset, :lql, "is required")

  defp lql_error(:source_required), do: "must name a source with f:<source>"
  defp lql_error({:unknown_source, name}), do: "unknown source: #{name}"
  defp lql_error({:invalid_lql, _reason}), do: "is invalid"
  defp lql_error({:unsupported_operator, op}), do: "uses an unsupported operator: #{op}"
  defp lql_error(:unsupported_backend), do: "source is not on a postgres backend"
  defp lql_error(reason), do: inspect(reason)

  defp validate_group_by(changeset, group_by) when is_list(group_by) do
    if length(group_by) <= @max_group_by and
         Enum.all?(group_by, &(is_binary(&1) and Regex.match?(@group_path, &1))) do
      changeset
    else
      Changeset.add_error(changeset, :group_by, "must be up to 3 dotted field paths")
    end
  end

  defp validate_group_by(changeset, _group_by),
    do: Changeset.add_error(changeset, :group_by, "must be a list")

  defp validate_identifiers(changeset, identifiers)
       when is_map(identifiers) and map_size(identifiers) > 0 do
    if Enum.all?(identifiers, fn {k, v} ->
         Regex.match?(~r/^[a-z0-9_]{1,64}$/, k) and is_binary(v)
       end),
       do: changeset,
       else: Changeset.add_error(changeset, :identifiers, "must map identifier keys to strings")
  end

  defp validate_identifiers(changeset, _identifiers),
    do: Changeset.add_error(changeset, :identifiers, "is required")

  defp preview_lql(lql, group_by) when is_binary(lql) and is_list(group_by) do
    group_by
    |> Enum.filter(&is_binary/1)
    |> Enum.map(fn path ->
      key = path |> String.split(".") |> List.last()
      "#{lql_path(path)}:{{#{key}}}"
    end)
    |> then(&Enum.join([lql | &1], " "))
  end

  defp preview_lql(lql, _group_by), do: lql

  defp lql_path("metadata." <> rest), do: "m." <> rest
  defp lql_path(path), do: path

  defp unique_rule_key(user, name) when is_binary(name) do
    base =
      "custom." <>
        (name
         |> String.downcase()
         |> String.replace(~r/[^a-z0-9]+/, "-")
         |> String.trim("-")
         |> String.slice(0, 64)
         |> then(&if(&1 == "", do: "rule", else: &1)))

    taken =
      AlertQuery
      |> where([a], a.user_id == ^user.id and like(a.rule_key, ^"#{base}%"))
      |> select([a], a.rule_key)
      |> Repo.all()
      |> MapSet.new()

    Stream.iterate(1, &(&1 + 1))
    |> Stream.map(fn
      1 -> base
      n -> "#{base}-#{n}"
    end)
    |> Enum.find(&(not MapSet.member?(taken, &1)))
  end

  defp unique_rule_key(_user, _name), do: nil
end
