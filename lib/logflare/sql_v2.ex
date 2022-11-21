defmodule Logflare.SqlV2 do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User
  alias __MODULE__.Parser
  @typep input :: String.t() | {String.t(), String.t()}
  @spec transform(input(), User.t()) :: {:ok, String.t()}
  def transform(
        input,
        %{
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id
        } = user
      ) do
    {query, sandboxed_query} =
      case input do
        q when is_binary(q) -> {q, nil}
        other when is_tuple(other) -> other
      end

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

    with {:ok, statements} <- Parser.parse(query),
         {:ok, sandboxed_query_ast} <-
           (case sandboxed_query do
              q when is_binary(q) -> Parser.parse(q)
              _ -> {:ok, nil}
            end),
         :ok <- maybe_validate_sandboxed_query_ast(sandboxed_query_ast) do
      data = %{
        project_id: project_id,
        dataset_id: dataset_id,
        sources: sources,
        source_mapping: source_mapping,
        source_names: Map.keys(source_mapping),
        sandboxed_query: sandboxed_query,
        sandboxed_query_ast: sandboxed_query_ast
      }

      statements
      |> do_transform(data)
      |> Parser.to_string()
    end
  end

  defp maybe_validate_sandboxed_query_ast(ast) when is_list(ast) do
    Enum.reduce(ast, :ok, fn
      statement, :ok ->
        cond do
          has_wildcard_in_select(statement) ->
            {:error, "restricted wildcard (*) in a result column"}

          true ->
            :ok
        end

      _statement, err ->
        err
    end)
  end

  defp maybe_validate_sandboxed_query_ast(_), do: :ok

  defp has_wildcard_in_select(statement), do: has_wildcard_in_select(statement, false)
  defp has_wildcard_in_select(_kv, true), do: true

  defp has_wildcard_in_select({"Select", %{"projection" => proj}}, _acc) do
    if("Wildcard" in proj, do: true, else: false)
  end

  defp has_wildcard_in_select(kv, acc) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.map(fn kv -> has_wildcard_in_select(kv, acc) end)
    |> Enum.any?()
  end

  defp has_wildcard_in_select({_k, v}, acc) when is_list(v) or is_map(v) do
    has_wildcard_in_select(v, acc)
  end

  defp has_wildcard_in_select(_kv, acc), do: acc

  defp do_transform(statements, data) when is_list(statements) do
    statements
    |> Enum.map(fn statement ->
      statement
      |> replace_names(data)
      |> replace_sandboxed_query(data)
      |> Map.new()
    end)
  end

  defp replace_names({"Table" = k, %{"name" => names} = v}, data) do
    new_name_list =
      for name_map <- names do
        name_value = Map.get(name_map, "value")

        to_merge =
          if name_value in data.source_names do
            %{"value" => transform_name(name_value, data), "quote_style" => "`"}
          else
            quote_style = Map.get(name_map, "quote_style")
            %{"value" => name_value, "quote_style" => quote_style}
          end

        Map.merge(name_map, to_merge)
      end

    {k, %{v | "name" => new_name_list}}
  end

  defp replace_names({"CompoundIdentifier" = k, [first | other]}, data) do
    value = Map.get(first, "value")

    new_identifier =
      if value in data.source_names do
        Map.merge(
          first,
          %{"value" => transform_name(value, data), "quote_style" => "`"}
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

  # ignore the queries inside of the CTE
  defp replace_sandboxed_query({"query", %{"body" => _}} = kv, _data), do: kv

  # only replace the top level query
  defp replace_sandboxed_query(
         {
           "body" = k,
           %{"Select" => _}
         },
         %{sandboxed_query: sandboxed_query, sandboxed_query_ast: ast} = data
       )
       when is_binary(sandboxed_query) do
    statements = do_transform(ast, %{data | sandboxed_query: nil})

    replacement_body =
      statements
      |> List.first()
      |> get_in(["Query", "body"])

    {k, replacement_body}
  end

  defp replace_sandboxed_query({k, v}, data) when is_list(v) or is_map(v) do
    {k, replace_sandboxed_query(v, data)}
  end

  defp replace_sandboxed_query(kv, data) when is_list(kv) do
    Enum.map(kv, fn kv -> replace_sandboxed_query(kv, data) end)
  end

  defp replace_sandboxed_query(kv, data) when is_map(kv) do
    Enum.map(kv, fn kv -> replace_sandboxed_query(kv, data) end) |> Map.new()
  end

  defp replace_sandboxed_query(kv, _data), do: kv

  defp transform_name(relname, data) do
    source = Enum.find(data.sources, fn s -> s.name == relname end)

    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    ~s(#{data.project_id}.#{data.dataset_id}.#{token})
  end
end
