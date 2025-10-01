defmodule Logflare.Sql do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Alerting.AlertQuery
  alias Logflare.Endpoints
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.Sql.AstUtils
  alias Logflare.Sql.DialectTransformer
  alias Logflare.Sql.DialectTranslation
  alias Logflare.Sql.Parser
  alias Logflare.User

  @valid_query_languages ~w(bq_sql ch_sql pg_sql)a

  @typep query_language :: :bq_sql | :ch_sql | :pg_sql

  @doc """
  Converts a language atom to its corresponding dialect.

  ## Examples

      iex> Logflare.Sql.to_dialect(:bq_sql)
      "bigquery"

      iex> Logflare.Sql.to_dialect(:ch_sql)
      "clickhouse"

      iex> Logflare.Sql.to_dialect(:pg_sql)
      "postgres"
  """
  @spec to_dialect(query_language()) :: String.t()
  defdelegate to_dialect(language), to: DialectTransformer

  @doc """
  Expands entity names that match query names into a subquery
  """
  @spec expand_subqueries(
          query_language(),
          input :: String.t(),
          queries :: [AlertQuery.t() | Endpoints.Query.t()]
        ) ::
          {:ok, String.t()}
  def expand_subqueries(_language, input, []), do: {:ok, input}

  def expand_subqueries(language, input, queries)
      when language in @valid_query_languages and is_non_empty_binary(input) and is_list(queries) do
    with parser_dialect <- to_dialect(language),
         {:ok, statements} <- Parser.parse(parser_dialect, input),
         eligible_queries <- Enum.filter(queries, &(&1.language == language)) do
      statements
      |> replace_names_with_subqueries(%{
        language: language,
        queries: eligible_queries
      })
      |> Parser.to_string()
    end
  end

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
  @spec transform(
          language :: query_language(),
          query :: input(),
          user_or_user_id :: User.t() | pos_integer() | nil
        ) ::
          {:ok, String.t()} | {:error, String.t()}
  def transform(lang, input, user_id) when is_integer(user_id) do
    user = Logflare.Users.get(user_id)
    transform(lang, input, user)
  end

  # clickhouse and postgres
  def transform(language, query, %User{} = user) when language in ~w(ch_sql pg_sql)a do
    sql_dialect = to_dialect(language)
    sources = Sources.list_sources_by_user(user)
    source_mapping = source_mapping(sources)

    Logger.metadata(query_string: query)

    with {:ok, statements} <- Parser.parse(sql_dialect, query) do
      statements
      |> do_transform(%{
        sources: sources,
        source_mapping: source_mapping,
        source_names: Map.keys(source_mapping),
        dialect: sql_dialect,
        ast: statements
      })
      |> Parser.to_string()
    end
  end

  # default to bq_sql
  def transform(lang, input, %User{} = user) when lang in [:bq_sql, nil] do
    {query, sandboxed_query} =
      case input do
        q when is_non_empty_binary(q) -> {q, nil}
        other when is_tuple(other) -> other
      end

    sources = Sources.list_sources_by_user(user)
    source_mapping = source_mapping(sources)

    Logger.metadata(query_string: query)

    with {:ok, statements} <- Parser.parse("bigquery", query),
         {:ok, sandboxed_query_ast} <- sandboxed_ast(sandboxed_query, "bigquery"),
         base_data = %{
           sources: sources,
           source_mapping: source_mapping,
           source_names: Map.keys(source_mapping),
           sandboxed_query: sandboxed_query,
           sandboxed_query_ast: sandboxed_query_ast,
           ast: statements,
           dialect: "bigquery"
         },
         data = DialectTransformer.BigQuery.build_transformation_data(user, base_data),
         :ok <- validate_query(statements, data),
         :ok <- maybe_validate_sandboxed_query_ast({statements, sandboxed_query_ast}, data) do
      data = %{data | sandboxed_query_ast: sandboxed_query_ast}

      statements
      |> do_transform(data)
      |> Parser.to_string()
    end
  end

  # Handle nil user case (e.g., during form validation)
  def transform(language, query, nil) when language in [:ch_sql, :pg_sql, :bq_sql, nil] do
    {:ok, query}
  end

  @doc """
  Translates BigQuery SQL to PostgreSQL SQL.
  """
  @spec translate(
          from :: :bq_sql,
          to :: :pg_sql,
          query :: String.t(),
          schema_prefix :: String.t() | nil
        ) :: {:ok, String.t()} | {:error, String.t()}
  def translate(:bq_sql, :pg_sql, query, schema_prefix \\ nil) when is_non_empty_binary(query) do
    DialectTranslation.translate_bq_to_pg(query, schema_prefix)
  end

  @doc """
  Returns a name-token mapping of all sources detected in the query.

  Excludes any unrecognized names (such as fully-qualified names).

  ### Example

    iex> sources("select a from my_table", %User{...})
    {:ok, %{"my_table" => "abced-weqqwe-..."}}
  """
  @spec sources(query :: String.t(), user :: User.t(), opts :: Keyword.t()) ::
          {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  def sources(query, user, opts \\ []) when is_list(opts) do
    dialect = Keyword.get(opts, :dialect, "bigquery")
    sources = Sources.list_sources_by_user(user)
    source_names = for s <- sources, do: s.name

    source_mapping =
      for source <- sources, into: %{} do
        {source.name, source}
      end

    with {:ok, ast} <- Parser.parse(dialect, query),
         names <-
           ast
           |> find_all_source_names()
           |> Enum.filter(fn name -> name in source_names end) do
      sources =
        names
        |> Enum.map(fn name ->
          token =
            source_mapping
            |> Map.get(name)
            |> Map.get(:token)
            |> then(fn
              v when is_atom_value(v) -> Atom.to_string(v)
              v -> v
            end)

          {name, token}
        end)
        |> Map.new()

      {:ok, sources}
    end
  end

  @doc """
  Extract out parameters from the SQL string.

  ### Example

    iex> query = "select f.to from my_table f where f.to = @something"
    iex> parameters(query)
    {:ok, ["something"]}
  """
  @spec parameters(String.t(), Keyword.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def parameters(query, opts \\ []) when is_list(opts) do
    dialect = Keyword.get(opts, :dialect, "bigquery")

    with {:ok, ast} <- Parser.parse(dialect, query) do
      {:ok, extract_all_parameters(ast)}
    end
  end

  @doc """
  Returns parameter positions mapped to their names.

  ### Example
  iex> parameter_positions("select @test as testing")
  %{1 => "test"}
  """
  @spec parameter_positions(query :: String.t(), opts :: Keyword.t()) ::
          {:ok, %{integer() => String.t()}}
  def parameter_positions(query, opts \\ []) when is_non_empty_binary(query) and is_list(opts) do
    {:ok, parameters} = parameters(query, opts)
    {:ok, do_parameter_positions_mapping(query, parameters)}
  end

  @doc """
  Checks if a query contains a Common Table Expression (CTE).
  """
  @spec contains_cte?(query :: String.t(), opts :: Keyword.t()) :: boolean()
  def contains_cte?(query, opts \\ []) when is_list(opts) do
    dialect = Keyword.get(opts, :dialect, "bigquery")

    with {:ok, ast} <- Parser.parse(dialect, query),
         [_ | _] <- extract_cte_aliases(ast) do
      true
    else
      _ -> false
    end
  end

  @doc """
  Updates source names in a query based on a token mapping.

  ### Example

  iex> source_mapping("select a from old_table_name", %{"old_table_name"=> "abcde-fg123-..."}, %User{})
  {:ok, "select a from new_table_name"}
  """
  @spec source_mapping(
          query :: String.t(),
          user :: User.t() | pos_integer(),
          mapping :: map(),
          opts :: Keyword.t()
        ) ::
          {:ok, String.t()} | {:error, String.t()}
  def source_mapping(query, user, mapping, opts \\ [])

  def source_mapping(query, %User{id: user_id}, mapping, opts) do
    source_mapping(query, user_id, mapping, opts)
  end

  def source_mapping(query, user_id, mapping, opts) when is_list(opts) do
    dialect = Keyword.get(opts, :dialect, "bigquery")
    sources = Sources.list_sources_by_user(user_id)

    with {:ok, ast} <- Parser.parse(dialect, query) do
      ast
      |> replace_old_source_names(%{
        sources: sources,
        mapping: mapping
      })
      |> Parser.to_string()
    end
  end

  defp replace_names_with_subqueries(ast, data) do
    AstUtils.transform_recursive(ast, data, &do_replace_names_with_subqueries/2)
  end

  defp do_replace_names_with_subqueries(
         {"relation" = k,
          %{"Table" => %{"alias" => table_alias, "name" => table_name_values}} = v},
         data
       )
       when is_list(table_name_values) and is_map(data) do
    table_name =
      case table_name_values do
        [%{"value" => table_name}] ->
          table_name

        [%{"value" => _} | _] ->
          Enum.map_join(table_name_values, ".", & &1["value"])

        _ ->
          raise "Invalid table name"
      end

    query = Enum.find(data.queries, &(&1.name == table_name))

    if query do
      parser_dialect = to_dialect(data.language)
      {:ok, [%{"Query" => body}]} = Parser.parse(parser_dialect, query.query)

      {k,
       %{
         "Derived" => %{
           "alias" => table_alias,
           "lateral" => false,
           "subquery" => body
         }
       }}
    else
      {k, v}
    end
  end

  defp do_replace_names_with_subqueries(ast_node, _data), do: {:recurse, ast_node}

  defp sandboxed_ast(query, dialect) when is_non_empty_binary(query),
    do: Parser.parse(dialect, query)

  defp sandboxed_ast(_, _), do: {:ok, nil}

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
         :ok <- has_restricted_sources(cte_ast, ast),
         :ok <- validate_sandboxed_query_ast(ast, data) do
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
         sandboxed_query_ast: sandboxed_query_ast,
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_aliases(ast)

    sandboxed_cte_names =
      if sandboxed_query_ast, do: extract_cte_aliases(sandboxed_query_ast), else: []

    qualified_name =
      Enum.map_join(name, ".", fn %{"value" => part} -> part end)

    [qualified_name]
    # remove known names
    |> Enum.reject(fn name ->
      cond do
        name in cte_names ->
          true

        name in sandboxed_cte_names ->
          true

        name in source_names ->
          true

        SingleTenant.single_tenant?() and
            project_fully_qualified_name?(name, logflare_project_id) ->
          # single tenant mode, allow user to use the global BQ project id
          true

        # user bigquery id is set
        user_project_id != nil ->
          project_fully_qualified_name?(name, user_project_id)

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

        %{"ShowVariable" => _} ->
          true

        _ ->
          false
      end
    end

    restricted = for statement <- ast, res = check.(statement), res, do: res

    if Enum.empty?(restricted) do
      :ok
    else
      {:error, "Only SELECT queries allowed"}
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

    if Enum.empty?(restricted) do
      :ok
    else
      {:error, "Restricted function #{Enum.join(restricted, ", ")}"}
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

    sandboxed_cte_names = extract_cte_aliases(ast)

    unknown_table_names =
      for statement <- ast,
          from <- extract_all_from(statement),
          %{"value" => table_name} <- get_in(from, ["relation", "Table", "name"]),
          table_name not in aliases,
          table_name not in sandboxed_cte_names do
        table_name
      end

    if Enum.empty?(unknown_table_names) do
      :ok
    else
      {:error, "Table not found in CTE: (#{Enum.join(unknown_table_names, ", ")})"}
    end
  end

  defp validate_sandboxed_query_ast(ast, %{sandboxed_query: sandboxed_query} = data) do
    case extract_replacement_query_from_ast(ast, data) do
      {:ok, _replacement_query} ->
        :ok

      {:error, transformed_statements} ->
        Logger.warning(
          "Sandboxed query validation: would produce nil replacement query. Transform count: #{length(transformed_statements)}. First: #{inspect(List.first(transformed_statements))}",
          error_string: sandboxed_query
        )

        {:error, "Only SELECT queries allowed in sandboxed queries"}
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

  defp replace_names(ast, data) do
    AstUtils.transform_recursive(ast, data, &do_replace_names/2)
  end

  defp do_replace_names({"Table" = k, %{"name" => names} = v}, data) do
    transformer = DialectTransformer.for_dialect(data.dialect)
    dialect_quote_style = transformer.quote_style()

    qualified_name = Enum.map_join(names, ".", fn %{"value" => part} -> part end)

    new_name_list =
      if qualified_name in data.source_names do
        transformed_name = transformer.transform_source_name(qualified_name, data)

        [
          %{
            "quote_style" => dialect_quote_style,
            "value" => transformed_name
          }
        ]
      else
        names
      end

    {k, %{v | "name" => new_name_list}}
  end

  defp do_replace_names({"CompoundIdentifier" = k, [first | other]}, data) do
    value = Map.get(first, "value")
    transformer = DialectTransformer.for_dialect(data.dialect)

    new_identifier =
      if value in data.source_names do
        Map.merge(
          first,
          %{
            "value" => transformer.transform_source_name(value, data),
            "quote_style" => transformer.quote_style()
          }
        )
      else
        first
      end

    {k, [new_identifier | other]}
  end

  defp do_replace_names(ast_node, _data), do: {:recurse, ast_node}

  defp replace_sandboxed_query(ast, data) do
    AstUtils.transform_recursive(ast, data, &do_replace_sandboxed_query/2)
  end

  @spec extract_replacement_query_from_ast(list(), map()) ::
          {:ok, map()} | {:error, list()}
  defp extract_replacement_query_from_ast(ast, data) do
    transformed_statements = do_transform(ast, %{data | sandboxed_query: nil})

    replacement_query =
      transformed_statements
      |> List.first()
      |> get_in(["Query"])

    if is_nil(replacement_query) do
      {:error, transformed_statements}
    else
      {:ok, replacement_query}
    end
  end

  defp do_replace_sandboxed_query({"query", %{"body" => _}} = kv, _data), do: kv

  defp do_replace_sandboxed_query(
         {
           "Query" = k,
           %{"with" => %{"cte_tables" => _}} = sandbox_query
         },
         %{sandboxed_query: sandboxed_query, sandboxed_query_ast: ast} = data
       )
       when is_non_empty_binary(sandboxed_query) do
    case extract_replacement_query_from_ast(ast, data) do
      {:ok, %{"with" => with_clause} = replacement_query} when not is_nil(with_clause) ->
        {k, Map.merge(sandbox_query, %{"body" => %{"Query" => replacement_query}})}

      {:ok, replacement_query} ->
        {k, Map.merge(sandbox_query, Map.drop(replacement_query, ["with"]))}

      {:error, transformed_statements} ->
        Logger.warning(
          "Sandboxed query validation: would produce nil replacement query. Transform count: #{length(transformed_statements)}. First: #{inspect(List.first(transformed_statements))}",
          error_string: sandboxed_query
        )

        {k, sandbox_query}
    end
  end

  defp do_replace_sandboxed_query(ast_node, _data), do: {:recurse, ast_node}

  defp find_all_source_names(ast),
    do: find_all_source_names(ast, [], %{ast: ast})

  defp find_all_source_names({"Table", %{"name" => name}}, prev, %{
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_aliases(ast)

    # Join qualified table name parts back together (e.g., ["a", "b", "c"] -> "a.b.c")
    qualified_name = Enum.map_join(name, ".", fn %{"value" => part} -> part end)

    new_names =
      if qualified_name not in prev and qualified_name not in cte_names do
        [qualified_name]
      else
        []
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

  defp extract_cte_aliases(ast) do
    for statement <- ast,
        %{"alias" => %{"name" => %{"value" => cte_name}}} <-
          get_in(statement, ["Query", "with", "cte_tables"]) || [] do
      cte_name
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

  defp extract_all_parameters(ast) do
    AstUtils.collect_from_ast(ast, &do_extract_parameters/1) |> Enum.uniq()
  end

  defp do_extract_parameters({"Placeholder", "@" <> value}), do: {:collect, value}
  defp do_extract_parameters(_ast_node), do: :skip

  defp extract_all_from(ast) do
    AstUtils.collect_from_ast(ast, &do_extract_from/1)
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
  end

  defp do_extract_from({"from", from}) when is_list(from), do: {:collect, from}
  defp do_extract_from(_ast_node), do: :skip

  # returns true if the name is fully qualified and has the project id prefix.
  @spec project_fully_qualified_name?(table_name :: String.t(), project_id :: String.t()) ::
          boolean()
  defp project_fully_qualified_name?(_table_name, nil), do: false

  defp project_fully_qualified_name?(table_name, project_id)
       when is_non_empty_binary(project_id) do
    {:ok, regex} = Regex.compile("#{project_id}\\..+\\..+")
    Regex.match?(regex, table_name)
  end

  @spec source_mapping(sources :: [Logflare.Sources.Source.t()]) :: %{
          String.t() => Logflare.Sources.Source.t()
        }
  defp source_mapping(sources) do
    for source <- sources, into: %{} do
      {source.name, source}
    end
  end

  @spec do_parameter_positions_mapping(query :: String.t(), params :: [String.t()]) :: %{
          integer() => String.t()
        }
  defp do_parameter_positions_mapping(_query, []), do: %{}

  defp do_parameter_positions_mapping(query, params)
       when is_non_empty_binary(query) and is_list(params) do
    str =
      params
      |> Enum.uniq()
      |> Enum.join("|")

    regexp = Regex.compile!("@(#{str})(?:\\s|$|\\,|\\,|\\)|\\()")

    Regex.scan(regexp, query)
    |> Enum.with_index(1)
    |> Enum.reduce(%{}, fn {[_, param], index}, acc ->
      Map.put(acc, index, String.trim(param))
    end)
  end
end
