defmodule Logflare.SqlV2 do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User

  @spec transform(String.t(), User.t()) :: {:ok, String.t()}
  def transform(
        query,
        %{
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id
        } = user
      ) do
    project_id =
      if is_nil(project_id) do
        Application.get_env(:logflare, Logflare.Google)[:project_id]
      else
        project_id
      end

    dataset_id =
      if is_nil(dataset_id) do
        env = Application.get_env(:logflare, :env)
        "#{user.id}_#{env}"
      else
        dataset_id
      end

    {:ok, %{"stmts" => statements}} = EpgQuery.parse(query)

    sources = Sources.list_sources_by_user(user)

    source_mapping =
      for source <- sources, into: %{} do
        {source.name, source}
      end

    data = %{
      project_id: project_id,
      dataset_id: dataset_id,
      sources: sources,
      source_mapping: source_mapping,
      source_names: Map.keys(source_mapping)
    }

    statements
    |> do_transform(data)
    |> EpgQuery.to_string()
    # BigQuery related hacks
    |> case do
      {:ok, str} ->
        {:ok,
         str
         |> String.replace(~r/\"(`[^\s]+`)\"/, "\\g{1}")}

      other ->
        other
    end
  end

  defp do_transform([%{"stmt" => _} | _] = statements, data) do
    statements
    |> Enum.map(fn statement ->
      statement
      |> replace_names(data)
      |> Map.new()
    end)
  end

  defp replace_names({"RangeVar" = k, %{"relname" => relname} = v}, data) do
    transformed_name =
      if relname in data.source_names do
        transform_name(relname, data)
      else
        relname
      end

    {k, %{v | "relname" => transformed_name}}
  end

  defp replace_names({"ColumnRef" = k, %{"fields" => fields} = v}, data) do
    transformed_fields =
      for field <- fields, {type, value} <- field do
        new_value =
          cond do
            Map.get(value, "str") in data.source_names ->
              %{value | "str" => transform_name(Map.get(value, "str"), data)}

            true ->
              value
          end

        [{type, new_value}]
        |> Map.new()
      end

    {k, %{v | "fields" => transformed_fields}}
  end

  defp replace_names({k, v}, data) when is_list(v) or is_map(v) do
    {k, replace_names(v, data)}
  end

  defp replace_names(kv, data) when is_list(kv) do
    Enum.map(kv, fn kv -> replace_names(kv, data) end)
  end

  defp replace_names(kv, data) when is_map(kv) do
    Enum.map(kv, fn kv -> replace_names(kv, data) end) |> Map.new()
  end

  defp replace_names(kv, _data), do: kv

  defp transform_name(relname, data) do
    source = Enum.find(data.sources, fn s -> s.name == relname end)

    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    ~s(`#{data.project_id}.#{data.dataset_id}.#{token}`)
  end
end
