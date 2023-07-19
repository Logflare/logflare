defmodule Logflare.Sql do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.SingleTenant
  alias Logflare.Sql.Parser
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends.Adaptor.PostgresAdaptor

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
  @typep language :: :pg_sql | :bq_sql
  @spec transform(language(), input(), User.t() | pos_integer()) :: {:ok, String.t()}
  def transform(lang, input, user_id) when is_integer(user_id) do
    user = Logflare.Users.get(user_id)
    transform(lang, input, user)
  end

  def transform(:pg_sql, query, user) do
    sources = Sources.list_sources_by_user(user)
    source_mapping = source_mapping(sources)

    with {:ok, statements} <- Parser.parse("bigquery", query) do
      statements
      |> do_transform(%{
        sources: sources,
        source_mapping: source_mapping,
        source_names: Map.keys(source_mapping),
        dialect: "postgres",
        ast: statements
      })
      |> Parser.to_string()
    end
  end

  # default to bq_sql
  def transform(lang, input, %User{} = user) when lang in [:bq_sql, nil] do
    %_{bigquery_project_id: user_project_id, bigquery_dataset_id: user_dataset_id} = user

    {query, sandboxed_query} =
      case input do
        q when is_binary(q) -> {q, nil}
        other when is_tuple(other) -> other
      end

    sources = Sources.list_sources_by_user(user)
    source_mapping = source_mapping(sources)

    with {:ok, statements} <- Parser.parse("bigquery", query),
         data = %{
           logflare_project_id: Application.get_env(:logflare, Logflare.Google)[:project_id],
           user_project_id: user_project_id,
           logflare_dataset_id: User.generate_bq_dataset_id(user),
           user_dataset_id: user_dataset_id,
           sources: sources,
           source_mapping: source_mapping,
           source_names: Map.keys(source_mapping),
           sandboxed_query: sandboxed_query,
           sandboxed_query_ast: nil,
           ast: statements,
           dialect: "bigquery"
         },
         :ok <- validate_query(statements, data),
         {:ok, sandboxed_query_ast} <- sandboxed_ast(data),
         :ok <- maybe_validate_sandboxed_query_ast({statements, sandboxed_query_ast}, data) do
      data = %{data | sandboxed_query_ast: sandboxed_query_ast}

      statements
      |> do_transform(data)
      |> Parser.to_string()
    end
  end

  defp sandboxed_ast(%{sandboxed_query: q, dialect: dialect}) when is_binary(q),
    do: Parser.parse(dialect, q)

  defp sandboxed_ast(_), do: {:ok, nil}

  @doc """
  Performs a check if a query contains a CTE. returns true if it is, returns false if not
  """
  def contains_cte?(query, opts \\ []) do
    opts = Enum.into(opts, %{dialect: "bigquery"})

    with {:ok, ast} <- Parser.parse(opts.dialect, query),
         [_ | _] <- extract_cte_alises(ast) do
      true
    else
      _ -> false
    end
  end

  # applies to both ctes, sandboxed queries, and non-ctes
  defp validate_query(ast, data) when is_list(ast) do
    with :ok <- check_select_statement_only(ast),
         :ok <- check_single_query_only(ast),
         :ok <- has_restricted_functions(ast),
         :ok <- has_wildcard_in_select(ast),
         :ok <- check_all_sources_allowed(ast, data) do
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

  defp check_all_sources_allowed(statement, data),
    do: check_all_sources_allowed(statement, :ok, data)

  defp check_all_sources_allowed(_kv, {:error, _} = err, _data), do: err

  defp check_all_sources_allowed({"Table", %{"name" => name}}, _acc, %{
         source_names: source_names,
         user_project_id: user_project_id,
         logflare_project_id: logflare_project_id,
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_alises(ast)

    table_names = for %{"value" => table_name} <- name, do: table_name

    table_names
    # remove known names
    |> Enum.reject(fn name ->
      cond do
        name in cte_names ->
          true

        name in source_names ->
          true

        SingleTenant.single_tenant?() and
            is_project_fully_qualified_name(name, logflare_project_id) ->
          # single tenant mode, allow user to use the global BQ project id
          true

        # user bigquery id is set
        user_project_id != nil ->
          is_project_fully_qualified_name(name, user_project_id)

        # all else are unknown

        true ->
          false
      end
    end)
    |> case do
      [] ->
        :ok

      unknown ->
        {:error, "can't find source #{Enum.join(unknown, ", ")}"}
    end
  end

  defp check_all_sources_allowed(kv, acc, data) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc ->
      check_all_sources_allowed(kv, nested_acc, data)
    end)
  end

  defp check_all_sources_allowed({_k, v}, acc, data) when is_list(v) or is_map(v) do
    check_all_sources_allowed(v, acc, data)
  end

  defp check_all_sources_allowed(_kv, acc, _data), do: acc

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
      %{"Wildcard" => _} -> true
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
    dialect_quote_style =
      case data.dialect do
        "postgres" -> "\""
        "bigquery" -> "`"
      end

    new_name_list =
      for %{"value" => value, "quote_style" => quote_style} = name_map <- names do
        name_value =
          if value in data.source_names do
            Map.merge(
              name_map,
              %{
                "quote_style" => quote_style || dialect_quote_style,
                "value" => transform_name(value, data)
              }
            )
          else
            name_map
          end
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
           "Query" = k,
           %{"with" => %{"cte_tables" => _}} = sandbox_query
         },
         %{sandboxed_query: sandboxed_query, sandboxed_query_ast: ast} = data
       )
       when is_binary(sandboxed_query) do
    sandboxed_statements = do_transform(ast, %{data | sandboxed_query: nil})

    replacement_query =
      sandboxed_statements
      |> List.first()
      |> get_in(["Query"])
      |> Map.drop(["with"])

    {k, Map.merge(sandbox_query, replacement_query)}
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

  defp transform_name(relname, %{dialect: "postgres"} = data) do
    source = Enum.find(data.sources, fn s -> s.name == relname end)
    PostgresAdaptor.table_name(source)
  end

  defp transform_name(relname, %{dialect: "bigquery"} = data) do
    source = Enum.find(data.sources, fn s -> s.name == relname end)

    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    # byob bq
    project_id =
      if is_nil(data.user_project_id), do: data.logflare_project_id, else: data.user_project_id

    # byob bq
    dataset_id =
      if is_nil(data.user_dataset_id), do: data.logflare_dataset_id, else: data.user_dataset_id

    ~s(#{project_id}.#{dataset_id}.#{token})
  end

  @doc """
  Returns a name-uuid mapping of all sources detected from inside of the query.

  excludes any unrecognized names (such as fully-qualified names).

  ### Example

    iex> sources("select a from my_table", %User{...})
    {:ok, %{"my_table" => "abced-weqqwe-..."}}
  """
  @spec sources(String.t(), User.t()) :: {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  def sources(query, user, opts \\ []) do
    opts = Enum.into(opts, %{dialect: "bigquery"})

    sources = Sources.list_sources_by_user(user)
    source_names = for s <- sources, do: s.name

    source_mapping =
      for source <- sources, into: %{} do
        {source.name, source}
      end

    sources =
      with {:ok, ast} <- Parser.parse(opts.dialect, query),
           names <-
             ast
             |> find_all_source_names()
             |> Enum.filter(fn name -> name in source_names end) do
        names
        |> Enum.map(fn name ->
          token =
            source_mapping
            |> Map.get(name)
            |> Map.get(:token)
            |> then(fn
              v when is_atom(v) -> Atom.to_string(v)
              v -> v
            end)

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
  def source_mapping(query, user, mapping, opts \\ [])

  def source_mapping(query, %Logflare.User{id: user_id}, mapping, opts) do
    source_mapping(query, user_id, mapping, opts)
  end

  def source_mapping(query, user_id, mapping, opts) do
    opts = Enum.into(opts, %{dialect: "bigquery"})
    sources = Sources.list_sources_by_user(user_id)

    with {:ok, ast} <- Parser.parse(opts.dialect, query) do
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
  def parameters(query, opts \\ []) do
    opts = Enum.into(opts, %{dialect: "bigquery"})

    with {:ok, ast} <- Parser.parse(opts.dialect, query) do
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

  # returns true if the name is fully qualified and has the project id prefix.
  defp is_project_fully_qualified_name(_table_name, nil), do: false

  defp is_project_fully_qualified_name(table_name, project_id) when is_binary(project_id) do
    {:ok, regex} = Regex.compile("#{project_id}\\..+\\..+")
    Regex.match?(regex, table_name)
  end

  defp source_mapping(sources) do
    for source <- sources, into: %{} do
      {source.name, source}
    end
  end

  def translate(:bq_sql, :pg_sql, query) when is_binary(query) do
    {:ok, stmts} = Parser.parse("bigquery", query)

    for ast <- stmts do
      ast
      |> bq_to_pg_quote_style()
      |> bq_to_pg_field_references()
      |> bq_to_pg_convert_functions()
    end
    |> Parser.to_string()
  end

  # traverse ast to convert all functions
  defp bq_to_pg_convert_functions({k, v} = kv)
       when k in ["Function", "AggregateExpressionWithFilter"] do
    function_name = v |> get_in(["name", Access.at(0), "value"]) |> String.downcase()

    case function_name do
      "countif" ->
        filter = get_in(v, ["args", Access.at(0), "Unnamed", "Expr"])

        {"AggregateExpressionWithFilter",
         %{
           "expr" => %{
             "Function" => %{
               "args" => [%{"Unnamed" => "Wildcard"}],
               "distinct" => false,
               "name" => [%{"quote_style" => nil, "value" => "count"}],
               "over" => nil,
               "special" => false
             }
           },
           "filter" => filter
         }}

      _ ->
        kv
    end
  end

  defp bq_to_pg_convert_functions({k, v}) when is_list(v) or is_map(v) do
    {k, bq_to_pg_convert_functions(v)}
  end

  defp bq_to_pg_convert_functions(kv) when is_list(kv) do
    Enum.map(kv, fn kv -> bq_to_pg_convert_functions(kv) end)
  end

  defp bq_to_pg_convert_functions(kv) when is_map(kv) do
    Enum.map(kv, fn kv -> bq_to_pg_convert_functions(kv) end) |> Map.new()
  end

  defp bq_to_pg_convert_functions(kv), do: kv

  defp bq_to_pg_quote_style(ast) do
    from =
      ast
      |> get_in(["Query", "body", "Select", "from"])
      |> Enum.map(fn from ->
        {_, updated} =
          get_and_update_in(from, ["relation", "Table", "name"], fn [%{"value" => source}] = value ->
            {value, [%{"quote_style" => "\"", "value" => source}]}
          end)

        updated
      end)

    input = put_in(ast, ["Query", "body", "Select", "from"], from)
    input
  end

  defp bq_to_pg_field_references(ast) do
    joins = get_in(ast, ["Query", "body", "Select", "from", Access.at(0), "joins"]) || []
    cleaned_joins = Enum.filter(joins, fn join -> get_in(join, ["relation", "UNNEST"]) == nil end)

    alias_path_mappings = get_bq_alias_path_mappings(ast)

    ast
    |> traverse_convert_identifiers(alias_path_mappings)
    |> then(fn
      ast when joins != [] ->
        put_in(ast, ["Query", "body", "Select", "from", Access.at(0), "joins"], cleaned_joins)

      ast ->
        ast
    end)
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => join_alias}, %{"value" => key} | _]} = i,
         alias_path_mappings
       ) do
    path = "{#{alias_path_mappings[join_alias]},#{key}}"

    %{
      "JsonAccess" => %{
        "left" => %{"Identifier" => %{"quote_style" => nil, "value" => "body"}},
        "operator" => "HashArrow",
        "right" => %{"Value" => %{"SingleQuotedString" => path}}
      }
    }
  end

  defp convert_keys_to_json_query(%{"Identifier" => %{"value" => name}} = i, _alias_path_mappings) do
    %{
      "JsonAccess" => %{
        "left" => %{"Identifier" => %{"quote_style" => nil, "value" => "body"}},
        "operator" => "Arrow",
        "right" => %{"Value" => %{"SingleQuotedString" => name}}
      }
    }
  end

  defp get_identifier_alias(%{
         "CompoundIdentifier" => [%{"value" => _join_alias}, %{"value" => key} | _]
       }) do
    key
  end

  defp get_identifier_alias(%{"Identifier" => %{"value" => name}}) do
    name
  end

  defp get_bq_alias_path_mappings(ast) do
    table_map =
      ast
      |> get_in(["Query", "body", "Select", "from", Access.at(0), "relation", "Table"])

    table_alias = get_in(table_map, ["alias", "name", "value"])

    joins =
      ast
      |> get_in(["Query", "body", "Select", "from", Access.at(0), "joins"]) || []

    Enum.reduce(joins, %{}, fn
      %{
        "relation" => %{
          "UNNEST" => %{
            "array_expr" => %{"CompoundIdentifier" => identifiers},
            "alias" => %{"name" => %{"value" => alias_name}}
          }
        }
      } = join,
      acc ->
        arr_path = for i <- identifiers, value = i["value"], value != table_alias, do: value

        str_path = Enum.join(arr_path, ",")

        Map.put(acc, alias_name, str_path)

      _join, acc ->
        acc
    end)
  end

  # auto set the column alias if not set
  defp traverse_convert_identifiers(
         {"UnnamedExpr", identifier},
         alias_path_mappings
       )
       when is_map_key(identifier, "CompoundIdentifier") or is_map_key(identifier, "Identifier") do
    {"ExprWithAlias",
     %{
       "alias" => %{"quote_style" => nil, "value" => get_identifier_alias(identifier)},
       "expr" => convert_keys_to_json_query(identifier, alias_path_mappings)
     }}
  end

  defp traverse_convert_identifiers({k, v}, alias_path_mappings)
       when k in ["CompoundIdentifier", "Identifier"] do
    convert_keys_to_json_query(%{k => v}, alias_path_mappings)
    |> Map.to_list()
    |> List.first()
  end

  defp traverse_convert_identifiers({k, v}, alias_path_mappings) when is_list(v) or is_map(v) do
    {k, traverse_convert_identifiers(v, alias_path_mappings)}
  end

  defp traverse_convert_identifiers(kv, alias_path_mappings) when is_list(kv) do
    Enum.map(kv, fn kv -> traverse_convert_identifiers(kv, alias_path_mappings) end)
  end

  defp traverse_convert_identifiers(kv, alias_path_mappings) when is_map(kv) do
    Enum.map(kv, fn kv -> traverse_convert_identifiers(kv, alias_path_mappings) end) |> Map.new()
  end

  defp traverse_convert_identifiers(kv, _data), do: kv
end
