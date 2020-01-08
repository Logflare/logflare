defmodule Logflare.Rules do
  @moduledoc false
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Rule
  import Ecto.Query
  require Logger

  def create_rule(source, params) do
    changeset =
      source
      |> Repo.preload([:rules], force: true)
      |> Ecto.build_assoc(:rules)
      |> Rule.changeset(params)

    case Repo.insert(changeset) do
      {:ok, rule} ->
        {:ok, rule}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, changeset}
    end
  end

  def delete_rule!(rule_id) do
    Rule |> Repo.get!(rule_id) |> Repo.delete!()
  end

  def upgrade_source_rules_from_regex_to_lql(source_id) do
    rules =
      Rule
      |> where([r], is_nil(r.lql_filters) and not is_nil(r.regex) and r.source_id == ^source_id)
      |> select([r], r)
      |> Repo.all()

    for rule <- rules do
      rule
      |> Rule.regex_to_lql_upgrade_changeset()
      |> Repo.update()
      |> case do
        {:ok, r} ->
          Logger.info("Rule #{r.id} for source #{r.source_id} upgraded to LQL filter")

        {:error, changeset} ->
          Logger.error(
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade, error: #{
              inspect(changeset.errors)
            }"
          )
      end
    end
  end

  def upgrade_all_source_rules_to_lql() do
    rules =
      Rule
      |> where([r], is_nil(r.lql_filters) and not is_nil(r.regex))
      |> select([r], r)
      |> Repo.all()

    for rule <- rules do
      rule
      |> Rule.regex_to_lql_upgrade_changeset()
      |> Repo.update()
      |> case do
        {:ok, r} ->
          Logger.info("Rule #{r.id} for source #{r.source_id} upgraded to LQL filter")

        {:error, changeset} ->
          Logger.error(
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade, error: #{
              inspect(changeset.errors)
            }"
          )
      end
    end
  end
end
