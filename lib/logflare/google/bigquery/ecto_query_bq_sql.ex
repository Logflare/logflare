defmodule Logflare.EctoQueryBQ.SQL do
  @moduledoc false
  alias Logflare.Repo
  alias Logflare.Logs.BigQuery.SearchParamTypes

  def to_sql_params(query) do
    {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

    sql = pg_sql_to_bq_sql(sql)
    params = Enum.map(params, &pg_param_to_bq_param/1)

    {sql, params}
  end

  def pg_sql_to_bq_sql(sql) do
    sql
    # replaces PG-style to BQ-style positional parameters
    |> String.replace(~r/\$\d+/, "?")
    # removes double quotes around the names after the dot
    |> String.replace(~r/\."([\w\.]+)"/, ".\\1")
    # removes double quotes around the qualified BQ table id
    |> String.replace(~r/FROM\s+"(.+)"/, "FROM \\1")
    # removes double quotes around the alias
    |> String.replace(~r/AS\s+"(\w+)"/, "AS \\1")
  end

  def pg_param_to_bq_param(_pg_sql_param = param) do
    alias GoogleApi.BigQuery.V2.Model
    alias Model.QueryParameter, as: Param
    alias Model.QueryParameterType, as: Type
    alias Model.QueryParameterValue, as: Value

    param =
      case param do
        %NaiveDateTime{} -> to_string(param)
        %DateTime{} -> to_string(param)
        %Date{} -> to_string(param)
        param -> param
      end

  def sql_params_to_sql({sql, params}) do
    Enum.reduce(params, sql, fn param, sql ->
      type = param.parameterType.type
      value = param.parameterValue.value

      case type do
        "STRING" ->
          String.replace(sql, "?", "'#{value}'", global: false)

        num when num in ~w(INTEGER FLOAT) ->
          String.replace(sql, "?", inspect(value), global: false)

        _ ->
          String.replace(sql, "?", inspect(value), global: false)
      end
    end)
  end

    %Param{
      parameterType: %Type{type: SearchParamTypes.to_schema_type(param)},
      parameterValue: %Value{value: param}
    }
  end
end
