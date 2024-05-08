defmodule Logflare.Rules do
  @moduledoc false
  import Ecto.Query
  require Logger

  alias Logflare.Lql
  alias Logflare.Repo
  alias Logflare.Rule
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends

  def list_rules(%Source{id: source_id}) do
    from(r in Rule, where: r.source_id == ^source_id)
    |> Repo.all()
  end

  def list_rules(%Backend{id: backend_id}) do
    from(r in Rule, where: r.backend_id == ^backend_id)
    |> Repo.all()
  end

  def create_rule(attrs \\ %{}) do
    %Rule{}
    |> Rule.changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, %Rule{backend_id: backend_id} = rule} = result when backend_id != nil ->
        if Backends.source_sup_started?(rule.source_id), do: SourceSup.start_rule_child(rule)
        result

      other ->
        other
    end
  end

  def update_rule(%Rule{} = rule, attrs) do
    rule
    |> Rule.changeset(attrs)
    |> Repo.update()
  end

  def get_rule(id), do: Repo.get(Rule, id)

  @spec create_rule(map(), Source.t()) :: {:ok, Rule.t()} | {:error, Ecto.Changeset.t() | binary}
  def create_rule(params, %Source{} = source) when is_map(params) do
    bq_schema =
      SourceSchemas.get_source_schema_by(source_id: source.id) |> Map.get(:bigquery_schema)

    lql_string = params["lql_string"]

    with {:ok, lql_filters} <- Lql.Parser.parse(lql_string, bq_schema),
         params = Map.put(params, "lql_filters", lql_filters),
         {:ok, rule} <- Rule.changeset(%Rule{source_id: source.id}, params) |> Repo.insert() do
      {:ok, rule}
    else
      {:error, %Ecto.Changeset{} = changeset} -> {:error, changeset}
      errtup -> errtup
    end
  end

  def delete_rule(rule) do
    res = Repo.delete(rule)
    if Backends.source_sup_started?(rule.source_id), do: SourceSup.stop_rule_child(rule)
    res
  end

  def delete_rule!(rule_id) do
    Rule |> Repo.get!(rule_id) |> Repo.delete!()
  end

  def upgrade_all_source_rules_to_next_lql_version() do
    Logger.info("Started upgrade of all source rules to next lql version...")

    rules =
      Rule
      |> where([r], not is_nil(r.lql_filters) and not is_nil(r.lql_string))
      |> Repo.all()

    for rule <- rules do
      source =
        rule.source_id
        |> Sources.get()
        |> Sources.put_bq_table_schema()

      case Lql.decode(rule.lql_string, source.bq_table_schema) do
        {:ok, lql_filters} ->
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

        {:error, error} ->
          Logger.error(
            "Rule #{rule.id} for source #{rule.source_id} failed to upgrade to new LQL filters format, LQL decoding error: #{inspect(error)}"
          )
      end
    end
  end
end
