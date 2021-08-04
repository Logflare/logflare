defmodule Logflare.Rules do
  @moduledoc false
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Rule
  alias Logflare.Lql
  alias Logflare.SourceSchemas
  import Ecto.Query
  require Logger

  @spec create_rule(map(), Source.t()) :: {:ok, Rule.t()} | {:error, Ecto.Changeset.t() | binary}
  def create_rule(params, %Source{} = source) when is_map(params) do
    bq_schema =
      SourceSchemas.Cache.get_source_schema_by(source_id: source.id) |> Map.get(:bigquery_schema)

    lql_string = params["lql_string"]

    with {:ok, lql_filters} <- Lql.Parser.parse(lql_string, bq_schema),
         params = Map.put(params, "lql_filters", lql_filters),
         {:ok, rule} <- Rule.changeset(%Rule{source_id: source.id}, params) |> Repo.insert() do
      {:ok, rule}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        {:error, changeset}

      errtup ->
        errtup
    end
  end

  def delete_rule!(rule_id) do
    Rule |> Repo.get!(rule_id) |> Repo.delete!()
  end

  @spec has_regex_rules?([Rule.t()]) :: boolean()
  def has_regex_rules?(rules) do
    Enum.reduce_while(rules, false, fn
      %Rule{regex_struct: rs}, _ when is_nil(rs) ->
        {:cont, false}

      %Rule{regex_struct: rs, lql_filters: lf}, _ when is_map(rs) and length(lf) >= 1 ->
        {:cont, false}

      %Rule{regex_struct: rs, lql_filters: []}, _ when is_map(rs) ->
        {:halt, true}
    end)
  end

  def upgrade_rules_to_lql(rules) do
    rules
    |> Enum.filter(fn %Rule{regex: regex, lql_filters: lql_filters} ->
      not is_nil(regex) and Enum.empty?(lql_filters)
    end)
    |> Enum.reduce_while(:ok, fn rule, _ ->
      rule
      |> Rule.regex_to_lql_upgrade_changeset()
      |> Repo.update()
      |> case do
        {:ok, r} ->
          Logger.info("Rule #{r.id} for source #{r.source_id} upgraded to LQL filters")
          {:cont, :ok}

        {:error, changeset} ->
          Logger.error(
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade to LQL, error: #{inspect(changeset.errors)}"
          )

          {:halt, {:error, changeset}}
      end
    end)
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
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade, error: #{inspect(changeset.errors)}"
          )
      end
    end
  end

  def upgrade_all_source_rules_to_next_lql_version() do
    Logger.info("Started upgrade of all source rules to next lql version...")

    rules =
      Rule
      |> where([r], not is_nil(r.lql_filters) and not is_nil(r.lql_string))
      |> select([r], r)
      |> Repo.all()

    for rule <- rules do
      source =
        rule.source_id
        |> Sources.get()
        |> Sources.put_bq_table_schema()

      with {:ok, lql_filters} <- Lql.decode(rule.lql_string, source.bq_table_schema) do
        if lql_filters != rule.lql_filters do
          rule
          |> Rule.changeset(%{lql_filters: lql_filters})
          |> Repo.update()
          |> case do
            {:ok, r} ->
              Logger.info(
                "Rule #{r.id} for source #{r.source_id} was successfully upgraded to new LQL filters format."
              )

            {:error, changeset} ->
              Logger.error(
                "Rule #{rule.id} for source #{rule.source_id} failed to upgrade to new LQL filters format, Repo update erro: #{inspect(changeset.errors)}"
              )
          end
        end
      else
        {:error, error} ->
          Logger.error(
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade to new LQL filters format, LQL decoding error: #{inspect(error)}"
          )
      end
    end
  end
end
