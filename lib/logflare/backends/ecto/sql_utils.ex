defmodule Logflare.Backends.Ecto.SqlUtils do
  @moduledoc """
  Shared utilities for converting Ecto queries to SQL for various backends.
  """

  require Logger

  @doc """
  Base function for converting Ecto queries to SQL using Ecto's PostgreSQL adapter.

  Returns the PostgreSQL SQL and parameters that can then be transformed by
  backend-specific logic.
  """
  @spec ecto_to_pg_sql(Ecto.Query.t()) :: {:ok, {String.t(), [any()]}} | {:error, String.t()}
  def ecto_to_pg_sql(%Ecto.Query{} = query) do
    try do
      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Logflare.Repo, query)
      {:ok, {sql, params}}
    rescue
      error ->
        Logger.warning("Failed to convert Ecto query to PostgreSQL SQL: #{inspect(error)}")
        {:error, "Could not convert Ecto query: #{Exception.message(error)}"}
    end
  end

  @doc """
  Normalizes date/datetime parameters to string format.
  """
  @spec normalize_datetime_param(any(), :clickhouse | :bigquery) :: any()
  def normalize_datetime_param(%NaiveDateTime{} = param, _backend), do: to_string(param)

  # TODO: ClickHouse DateTime64(6) without timezone can't parse ISO8601 with 'Z' suffix.
  # Consider using DateTime64(6, 'UTC') in schema instead to avoid this conversion.
  def normalize_datetime_param(%DateTime{} = param, :clickhouse) do
    param |> DateTime.to_naive() |> NaiveDateTime.to_string()
  end

  def normalize_datetime_param(%DateTime{} = param, _backend), do: to_string(param)
  def normalize_datetime_param(%Date{} = param, _backend), do: to_string(param)
  def normalize_datetime_param(param, _backend), do: param

  @doc """
  Converts PostgreSQL-style positional parameters ($1, $2, etc.) to question mark format.

  This is needed for backends that use `?` instead of `$n` parameters.
  """
  @spec pg_params_to_question_marks(String.t()) :: String.t()
  def pg_params_to_question_marks(sql) do
    String.replace(sql, ~r/\$\d+/, "?")
  end
end
