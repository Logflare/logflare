defmodule Logflare.SqlV2 do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User
  alias __MODULE__.Parser
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


    {:ok, statements} = Parser.parse(query)

    statements
    |> do_transform(data)
    |> Parser.to_string()
  end

  defp do_transform(statements, data) when is_list(statements) do
    statements
    |> Enum.map(fn statement ->
      statement
      |> replace_names(data)
      |> Map.new()
    end)
  end

  defp replace_names({"Table" = k, %{"name" => names} = v}, data) do
    new_name_list = for name_map <- names do
      name_value = Map.get(name_map, "value")
        new_name_value = if name_value in data.source_names do
          transform_name(name_value, data)
        else
          name_value
        end

      Map.merge(
        name_map,
        %{ "value"=> new_name_value, "quote_style"=> "`"}
      )

    end


    {k, %{v | "name" => new_name_list}}
  end

  defp replace_names({"CompoundIdentifier" = k, [first | other ] }, data) do
    value = Map.get(first, "value")
    new_identifier = if value in data.source_names do

      Map.merge(
        first,
        %{ "value"=> transform_name(value, data), "quote_style"=> "`"}
      )
    else
      first
    end


    {k, [new_identifier | other]}
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

    ~s(#{data.project_id}.#{data.dataset_id}.#{token})
  end
end
