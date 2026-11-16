defmodule Logflare.AlertIssues.IssueTypes do
  @moduledoc """
  Alert issue types are a user's alert queries with a `rule_key`. Each type carries
  whether it is silenced for a given set of identifiers.
  """

  import Ecto.Query

  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Repo
  alias Logflare.User

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
end
