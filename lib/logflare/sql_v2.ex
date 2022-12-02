defmodule Logflare.SqlV2 do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User
  alias __MODULE__.Parser

  @doc """
  Transforms and validates an SQL query for querying with bigquery.any()
  The resultant SQL is BigQuery compatible.


  DML is blocked
  - https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

  ### Example

    iex> transform("select a from my_table", %User{...})
    {:ok, "select a from `my_project.my_dataset.source_token`"}

  With a sandboxed query
    iex> cte = "..."
    iex> transform({cte, "select a from my_alias"}, %User{...})
    {:ok, "..."}
  """
  @typep input :: String.t() | {String.t(), String.t()}
  @spec transform(input(), User.t()) :: {:ok, String.t()}
  def transform(input, user_id) when is_integer(user_id) do
    user = Logflare.Users.get(user_id)
    transform(input, user)
  end
  def transform(
        input,
        %_{
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
         data = %{
           project_id: project_id,
           dataset_id: dataset_id,
           sources: sources,
           source_mapping: source_mapping,
           source_names: Map.keys(source_mapping),
           sandboxed_query: sandboxed_query,
           sandboxed_query_ast: nil,
           ast: statements
         },
         :ok <- validate_query(statements, data),
         {:ok, sandboxed_query_ast} <-
           (case sandboxed_query do
              q when is_binary(q) -> Parser.parse(q)
              _ -> {:ok, nil}
            end),
         :ok <- maybe_validate_sandboxed_query_ast({statements, sandboxed_query_ast}, data) do
      data = %{data | sandboxed_query_ast: sandboxed_query_ast}

      statements
      |> do_transform(data)
      |> Parser.to_string()
    end
  end

  # applies to both ctes, sandboxed queries, and non-ctes
  defp validate_query(ast, data) when is_list(ast) do
    with :ok <- check_select_statement_only(ast),
         :ok <- check_single_query_only(ast),
         :ok <- has_restricted_functions(ast),
         :ok <- has_wildcard_in_select(ast),
         :ok <- check_all_sources_known(ast, data) do
      :ok
    end
  end

  # applies only to the sandboed query
  defp maybe_validate_sandboxed_query_ast({cte_ast, ast}, data) when is_list(ast) do
    with :ok <- validate_query(ast, data),
         :ok <- has_restricted_sources(cte_ast, ast) do
      :ok
    end
  end

  defp maybe_validate_sandboxed_query_ast(_, _data), do: :ok

  defp check_all_sources_known(statement, data),
    do: check_all_sources_known(statement, :ok, data)

  defp check_all_sources_known(_kv, {:error, _} = err, _data), do: err

  defp check_all_sources_known({"Table", %{"name" => name}}, _acc, %{
         source_names: source_names,
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_alises(ast)

    unknown_table_names =
      for %{"value" => table_name} <- name,
          table_name not in source_names and table_name not in cte_names do
        table_name
      end

    if length(unknown_table_names) == 0 do
      :ok
    else
      {:error, "can't find source #{Enum.join(unknown_table_names, ", ")}"}
    end
  end

  defp check_all_sources_known(kv, acc, data) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc ->
      check_all_sources_known(kv, nested_acc, data)
    end)
  end

  defp check_all_sources_known({_k, v}, acc, data) when is_list(v) or is_map(v) do
    check_all_sources_known(v, acc, data)
  end

  defp check_all_sources_known(_kv, acc, _data), do: acc

  defp check_single_query_only([_stmt]), do: :ok

  defp check_single_query_only(_ast), do: {:error, "Only singular query allowed"}

  defp check_select_statement_only(ast) do
    check = fn input ->
      case input do
        %{"Insert" => _} ->
          true

        %{"Update" => _} ->
          true

        %{"Delete" => _} ->
          true

        %{"Truncate" => _} ->
          true

        %{"Merge" => _} ->
          true

        %{"Drop" => _} ->
          true

        _ ->
          false
      end
    end

    restricted = for statement <- ast, res = check.(statement), res, do: res

    if length(restricted) > 0 do
      {:error, "Only SELECT queries allowed"}
    else
      :ok
    end
  end

  defp has_restricted_functions(ast) when is_list(ast), do: has_restricted_functions(ast, :ok)

  defp has_restricted_functions({"Function", %{"name" => [%{"value" => _} | _] = names}}, :ok) do
    restricted =
      for name <- names,
          normalized = String.downcase(name["value"]),
          normalized in ["session_user", "external_query"] do
        normalized
      end

    if length(restricted) > 0 do
      {:error, "Restricted function #{Enum.join(restricted, ", ")}"}
    else
      :ok
    end
  end

  defp has_restricted_functions(kv, :ok = acc) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc -> has_restricted_functions(kv, nested_acc) end)
  end

  defp has_restricted_functions({_k, v}, :ok = acc) when is_list(v) or is_map(v) do
    has_restricted_functions(v, acc)
  end

  defp has_restricted_functions(_kv, acc), do: acc

  defp has_restricted_sources(cte_ast, ast) when is_list(ast) do
    aliases =
      for %{"Query" => %{"with" => %{"cte_tables" => tables}}} <- cte_ast,
          %{"alias" => %{"name" => %{"value" => table_alias}}} <- tables do
        table_alias
      end

    unknown_table_names =
      for statement <- ast,
          from <- get_in(statement, ["Query", "body", "Select", "from"]),
          %{"value" => table_name} <- get_in(from, ["relation", "Table", "name"]),
          table_name not in aliases do
        table_name
      end

    if length(unknown_table_names) == 0 do
      :ok
    else
      {:error, "Table not found in CTE: (#{Enum.join(unknown_table_names, ", ")})"}
    end
  end

  defp has_wildcard_in_select(statement),
    do: has_wildcard_in_select(statement, :ok)

  defp has_wildcard_in_select(_kv, {:error, _} = err), do: err

  defp has_wildcard_in_select({"projection", proj}, _acc) when is_list(proj) do
    proj
    |> Enum.any?(fn
      "Wildcard" -> true
      %{"QualifiedWildcard" => _} -> true
      _ -> false
    end)
    |> case do
      true -> {:error, "restricted wildcard (*) in a result column"}
      false -> :ok
    end
  end

  defp has_wildcard_in_select(kv, acc) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc -> has_wildcard_in_select(kv, nested_acc) end)
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

  @doc """
  Returns a name-uuid mapping of all sources detected from inside of the query

  ### Example

    iex> sources("select a from my_table", %User{...})
    {:ok, %{"my_table" => "abced-weqqwe-..."}}
  """
  @spec sources(String.t(), User.t()) :: {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  def sources(query, user) do
    sources = Sources.list_sources_by_user(user)

    source_mapping =
      for source <- sources, into: %{} do
        {source.name, source}
      end

    sources =
      with {:ok, ast} <- Parser.parse(query),
           names <- find_all_source_names(ast) do
        names
        |> Enum.map(fn name ->
          token =
            source_mapping
            |> Map.get(name)
            |> Map.get(:token)
            |> case do
              v when is_atom(v) ->
                Atom.to_string(v)

              v ->
                v
            end

          {name, token}
        end)
        |> Map.new()
      end

    {:ok, sources}
  end

  defp find_all_source_names(ast),
    do: find_all_source_names(ast, [], %{ast: ast})

  defp find_all_source_names({"Table", %{"name" => name}}, prev, %{
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_alises(ast)

    new_names =
      for %{"value" => table_name} <- name,
          table_name not in prev and table_name not in cte_names do
        table_name
      end

    new_names ++ prev
  end

  defp find_all_source_names(kv, acc, data) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc ->
      find_all_source_names(kv, nested_acc, data)
    end)
  end

  defp find_all_source_names({_k, v}, acc, data) when is_list(v) or is_map(v) do
    find_all_source_names(v, acc, data)
  end

  defp find_all_source_names(_kv, acc, _data), do: acc

  defp extract_cte_alises(ast) do
    for statement <- ast,
        %{"alias" => %{"name" => %{"value" => cte_name}}} <-
          get_in(statement, ["Query", "with", "cte_tables"]) || [] do
      cte_name
    end
  end

  @doc """
  Transforms and a stale query string with renamed sources to

  ### Example

  iex> source_mapping("select a from old_table_name", %{"old_table_name"=> "abcde-fg123-..."}, %User{})
  {:ok, "select a from new_table_name"}
  """
  def source_mapping(query, %Logflare.User{id: user_id}, mapping) do
    source_mapping(query, user_id, mapping)
  end

  def source_mapping(query, user_id, mapping) do
    sources = Sources.list_sources_by_user(user_id)

    with {:ok, ast} <- Parser.parse(query) do
      ast
      |> replace_old_source_names(%{
        sources: sources,
        mapping: mapping
      })
      |> Parser.to_string()
    end
  end

  defp replace_old_source_names({"Table" = k, %{"name" => names} = v}, %{
         sources: sources,
         mapping: mapping
       }) do
    new_name_list =
      for %{"value" => name_value} = name_map <- names do
        if name_value in Map.keys(mapping) do
          new_name = get_updated_source_name(name_value, mapping, sources)
          %{name_map | "value" => new_name}
        else
          name_map
        end
      end

    {k, %{v | "name" => new_name_list}}
  end

  defp replace_old_source_names({"CompoundIdentifier" = k, [first | other]}, %{
         sources: sources,
         mapping: mapping
       }) do
    value = Map.get(first, "value")

    new_first =
      if value in Map.keys(mapping) do
        new_name = get_updated_source_name(value, mapping, sources)
        %{first | "value" => new_name}
      else
        first
      end

    {k, [new_first | other]}
  end

  defp replace_old_source_names({k, v}, data) when is_list(v) or is_map(v) do
    {k, replace_old_source_names(v, data)}
  end

  defp replace_old_source_names(kv, data) when is_list(kv) do
    Enum.map(kv, fn kv -> replace_old_source_names(kv, data) end)
  end

  defp replace_old_source_names(kv, data) when is_map(kv) do
    Enum.map(kv, fn kv -> replace_old_source_names(kv, data) end) |> Map.new()
  end

  defp replace_old_source_names(kv, _data), do: kv

  defp get_updated_source_name(old_name, mapping, sources) do
    source = Enum.find(sources, fn s -> "#{s.token}" == mapping[old_name] end)
    source.name
  end

  @doc """
  Extract out parameters from the SQL string.

  ### Example

    iex> query = "select f.to from my_table f where f.to = @something"
    iex> parameters(query)
    {:ok, ["something"]}
  """
  def parameters(query) do
    with {:ok, ast} <- Parser.parse(query) do
      {:ok, extract_all_parameters(ast)}
    end
  end

  defp extract_all_parameters(ast),
    do: extract_all_parameters(ast, [])

  defp extract_all_parameters({"Placeholder", "@" <> value}, acc) do
    if value not in acc, do: [value | acc], else: acc
  end

  defp extract_all_parameters(kv, acc) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc ->
      extract_all_parameters(kv, nested_acc)
    end)
  end

  defp extract_all_parameters({_k, v}, acc) when is_list(v) or is_map(v) do
    extract_all_parameters(v, acc)
  end

  defp extract_all_parameters(_kv, acc), do: acc
end
