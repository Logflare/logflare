defmodule Logflare.SqlV2 do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User

  @spec transform(String.t(), User.t()) :: {:ok, String.t()}
  def transform(query, %{
    bigquery_project_id: project_id,
    bigquery_dataset_id: dataset_id,

  } =  user) do
    project_id = if is_nil(project_id) do
      Application.get_env(:logflare, Logflare.Google)[:project_id]
    else
      project_id
    end

    dataset_id = if is_nil(dataset_id) do
      append = Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]
      inspect(user.id) <> append
    else
      dataset_id
    end

    {:ok, %{"stmts"=> statements }} = EpgQuery.parse(query)
    statements
    |> Enum.map(&(&1["stmt"]))
    |> IO.inspect()
    {:ok, query}
  end


end
