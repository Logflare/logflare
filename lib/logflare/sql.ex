defmodule Logflare.Sql do
  @moduledoc """
  SQL parsing and transformation based on open source parser.

  This module provides the main interface with the rest of the app.
  """
  require Logger
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.SingleTenant
  alias Logflare.Sql.Parser
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Endpoints
  alias Logflare.Alerts.Alert

  @type language :: :pg_sql | :bq_sql

  @doc """
  Expands entity names that match query names into a subquery
  """
  @spec expand_subqueries(language(), String.t(), [Alert.t() | Endpoints.Query.t()]) ::
          {:ok, String.t()}
  def expand_subqueries(_language, input, []), do: {:ok, input}

  def expand_subqueries(language, input, queries)
      when is_atom(language) and is_list(queries) and is_binary(input) do
    parser_dialect =
      case language do
        :bq_sql -> "bigquery"
        :pg_sql -> "postgres"
      end

    with {:ok, statements} <- Parser.parse(parser_dialect, input),
         eligible_queries <- Enum.filter(queries, &(&1.language == language)) do
      statements
      |> replace_names_with_subqueries(%{
        language: language,
        queries: eligible_queries
      })
      |> Parser.to_string()
    end
  end

  defp replace_names_with_subqueries(
         {"relation" = k,
          %{"Table" => %{"alias" => table_alias, "name" => [%{"value" => table_name}]}} = v},
         data
       ) do
    query = Enum.find(data.queries, &(&1.name == table_name))

    if query do
      parser_language =
        case data.language do
          :pg_sql -> "postgres"
          :bq_sql -> "bigquery"
        end

      {:ok, [%{"Query" => body}]} = Parser.parse(parser_language, query.query)

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

  defp replace_names_with_subqueries({k, v}, data) when is_list(v) or is_map(v) do
    {k, replace_names_with_subqueries(v, data)}
  end

  defp replace_names_with_subqueries(kv, data) when is_list(kv) do
    Enum.map(kv, fn kv -> replace_names_with_subqueries(kv, data) end)
  end

  defp replace_names_with_subqueries(kv, data) when is_map(kv) do
    Map.new(kv, &replace_names_with_subqueries(&1, data))
  end

  defp replace_names_with_subqueries(kv, _data), do: kv

  # replaces all table names with subqueries

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
         {:ok, sandboxed_query_ast} <- sandboxed_ast(sandboxed_query, "bigquery"),
         data = %{
           logflare_project_id: Application.get_env(:logflare, Logflare.Google)[:project_id],
           user_project_id: user_project_id,
           logflare_dataset_id: User.generate_bq_dataset_id(user),
           user_dataset_id: user_dataset_id,
           sources: sources,
           source_mapping: source_mapping,
           source_names: Map.keys(source_mapping),
           sandboxed_query: sandboxed_query,
           sandboxed_query_ast: sandboxed_query_ast,
           ast: statements,
           dialect: "bigquery"
         },
         :ok <- validate_query(statements, data),
         :ok <- maybe_validate_sandboxed_query_ast({statements, sandboxed_query_ast}, data) do
      data = %{data | sandboxed_query_ast: sandboxed_query_ast}

      statements
      |> do_transform(data)
      |> Parser.to_string()
    end
  end

  defp sandboxed_ast(query, dialect) when is_binary(query),
    do: Parser.parse(dialect, query)

  defp sandboxed_ast(_, _), do: {:ok, nil}

  @doc """
  Performs a check if a query contains a CTE. returns true if it is, returns false if not
  """
  def contains_cte?(query, opts \\ []) do
    opts = Enum.into(opts, %{dialect: "bigquery"})

    with {:ok, ast} <- Parser.parse(opts.dialect, query),
         [_ | _] <- extract_cte_aliases(ast) do
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
         sandboxed_query_ast: sandboxed_query_ast,
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_aliases(ast)

    sandboxed_cte_names =
      if sandboxed_query_ast, do: extract_cte_aliases(sandboxed_query_ast), else: []

    table_names = for %{"value" => table_name} <- name, do: table_name

    table_names
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

    if replacement_query["with"] do
      # if the replacement query has a with clause, nest it in a Query node
      {k, Map.merge(sandbox_query, %{"body" => %{"Query" => replacement_query}})}
    else
      # if the replacement query does not have a with clause, just drop the with clause
      {k, Map.merge(sandbox_query, Map.drop(replacement_query, ["with"]))}
    end
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

    with {:ok, ast} <- Parser.parse(opts.dialect, query),
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
              v when is_atom(v) -> Atom.to_string(v)
              v -> v
            end)

          {name, token}
        end)
        |> Map.new()

      {:ok, sources}
    end
  end

  defp find_all_source_names(ast),
    do: find_all_source_names(ast, [], %{ast: ast})

  defp find_all_source_names({"Table", %{"name" => name}}, prev, %{
         ast: ast
       })
       when is_list(name) do
    cte_names = extract_cte_aliases(ast)

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

  defp extract_cte_aliases(ast) do
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
    if value in acc, do: acc, else: [value | acc]
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

  defp extract_all_from(ast), do: extract_all_from(ast, [])

  defp extract_all_from({"from", from}, acc) when is_list(from) do
    from ++ acc
  end

  defp extract_all_from(kv, acc) when is_list(kv) or is_map(kv) do
    kv
    |> Enum.reduce(acc, fn kv, nested_acc ->
      extract_all_from(kv, nested_acc)
    end)
  end

  defp extract_all_from({_k, v}, acc) when is_list(v) or is_map(v) do
    extract_all_from(v, acc)
  end

  defp extract_all_from(_kv, acc), do: acc

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

  @doc """
  Determine positions of all parameters

  ### Example
  iex> parameter_positions("select @test as testing")
  %{1 => "test"}
  """
  @spec parameter_positions(String.t()) :: %{integer() => String.t()}
  def parameter_positions(string) when is_binary(string) do
    {:ok, parameters} = parameters(string)
    {:ok, do_parameter_positions_mapping(string, parameters)}
  end

  def do_parameter_positions_mapping(_string, []), do: %{}

  def do_parameter_positions_mapping(string, params) when is_binary(string) and is_list(params) do
    str =
      params
      |> Enum.uniq()
      |> Enum.join("|")

    regexp = Regex.compile!("@(#{str})(?:\\s|$|\\,|\\,|\\)|\\()")

    Regex.scan(regexp, string)
    |> Enum.with_index(1)
    |> Enum.reduce(%{}, fn {[_, param], index}, acc ->
      Map.put(acc, index, String.trim(param))
    end)
  end

  def translate(:bq_sql, :pg_sql, query, schema_prefix \\ nil) when is_binary(query) do
    {:ok, stmts} = Parser.parse("bigquery", query)

    for ast <- stmts do
      ast
      |> bq_to_pg_convert_functions()
      |> bq_to_pg_field_references()
      |> pg_traverse_final_pass()
    end
    |> then(fn ast ->
      params = extract_all_parameters(ast)

      {:ok, query_string} =
        ast
        |> Parser.to_string()

      # explicitly set the schema prefix of the table
      replacement_pattern =
        if schema_prefix do
          ~s|"#{schema_prefix}"."log_events_\\g{2}"|
        else
          "\"log_events_\\g{2}\""
        end

      converted =
        query_string
        |> bq_to_pg_convert_parameters(params)
        # TODO: remove once sqlparser-rs bug is fixed
        # parser for postgres adds parenthesis to the end for postgres
        |> String.replace(~r/current\_timestamp\(\)/im, "current_timestamp")
        |> String.replace(~r/\"([\w\_\-]*\.[\w\_\-]+)\.([\w_]{36})"/im, replacement_pattern)

      Logger.debug(
        "Postgres translation is complete: #{query} | \n output: #{inspect(converted)}"
      )

      {:ok, converted}
    end)
  end

  # use regexp to convert the string to
  defp bq_to_pg_convert_parameters(string, []), do: string

  defp bq_to_pg_convert_parameters(string, params) do
    do_parameter_positions_mapping(string, params)
    |> Map.to_list()
    |> Enum.sort_by(fn {i, _v} -> i end, :asc)
    |> Enum.reduce(string, fn {index, param}, acc ->
      Regex.replace(~r/@#{param}(?!:\s|$)/, acc, "$#{index}::text", global: false)
    end)
  end

  # traverse ast to convert all functions
  defp bq_to_pg_convert_functions({k, v} = kv)
       when k in ["Function", "AggregateExpressionWithFilter"] do
    function_name = v |> get_in(["name", Access.at(0), "value"]) |> String.downcase()

    case function_name do
      "regexp_contains" ->
        string =
          get_in(v, ["args", Access.at(0), "Unnamed", "Expr"])
          |> update_in(["Value"], &%{"SingleQuotedString" => &1["DoubleQuotedString"]})

        pattern =
          get_in(v, ["args", Access.at(1), "Unnamed", "Expr"])
          |> update_in(["Value"], &%{"SingleQuotedString" => &1["DoubleQuotedString"]})

        {"BinaryOp", %{"left" => string, "op" => "PGRegexMatch", "right" => pattern}}

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
           "filter" => bq_to_pg_convert_functions(filter)
         }}

      "timestamp_sub" ->
        to_sub = get_in(v, ["args", Access.at(0), "Unnamed", "Expr"])
        interval = get_in(v, ["args", Access.at(1), "Unnamed", "Expr", "Interval"])
        interval_type = interval["leading_field"]
        interval_value_str = get_in(interval, ["value", "Value", "Number", Access.at(0)])
        pg_interval = String.downcase("#{interval_value_str} #{interval_type}")

        {"BinaryOp",
         %{
           "left" => bq_to_pg_convert_functions(to_sub),
           "op" => "Minus",
           "right" => %{
             "Interval" => %{
               "fractional_seconds_precision" => nil,
               "last_field" => nil,
               "leading_field" => nil,
               "leading_precision" => nil,
               "value" => %{"Value" => %{"SingleQuotedString" => pg_interval}}
             }
           }
         }}

      "timestamp_trunc" ->
        to_trunc = get_in(v, ["args", Access.at(0), "Unnamed", "Expr"])

        interval_type =
          get_in(v, ["args", Access.at(1), "Unnamed", "Expr", "Identifier", "value"])
          |> String.downcase()

        field_arg =
          if timestamp_identifier?(to_trunc) do
            at_time_zone(to_trunc)
          else
            to_trunc
          end

        {k,
         %{
           v
           | "args" => [
               %{
                 "Unnamed" => %{"Expr" => %{"Value" => %{"SingleQuotedString" => interval_type}}}
               },
               %{
                 "Unnamed" => %{"Expr" => field_arg}
               }
             ],
             "name" => [%{"quote_style" => nil, "value" => "date_trunc"}]
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

  # between operator should have balues cast to numeric
  defp pg_traverse_final_pass({"Between" = k, %{"expr" => expr} = v}) do
    new_expr = expr |> pg_traverse_final_pass() |> cast_to_numeric()
    {k, %{v | "expr" => new_expr}}
  end

  # handle binary operations comparison casting
  defp pg_traverse_final_pass(
         {"BinaryOp" = k,
          %{
            "left" => left,
            "right" => right,
            "op" => operator
          } = v}
       ) do
    # handle left/right numberic value comparisons
    is_numeric_comparison = numeric_value?(left) or numeric_value?(right)

    [left, right] =
      for expr <- [left, right] do
        cond do
          # skip if it is a value
          match?(%{"Value" => _}, expr) ->
            expr

          # convert the identifier side to number
          is_numeric_comparison and (identifier?(expr) or json_access?(expr)) ->
            expr
            |> cast_to_jsonb()
            |> jsonb_to_text()
            |> cast_to_numeric()

          timestamp_identifier?(expr) ->
            at_time_zone(expr)

          identifier?(expr) and operator == "Eq" ->
            # wrap with a cast to convert possible jsonb fields
            expr
            |> cast_to_jsonb()
            |> jsonb_to_text()

          true ->
            expr
        end
      end

    {k, %{v | "left" => left, "right" => right} |> pg_traverse_final_pass()}
  end

  # convert backticks to double quotes
  defp pg_traverse_final_pass({"quote_style" = k, "`"}), do: {k, "\""}
  # drop cross join unnest
  defp pg_traverse_final_pass({"joins" = k, joins}) do
    filtered_joins =
      for j <- joins,
          Map.get(j, "join_operator") != "CrossJoin",
          !is_map_key(Map.get(j, "relation"), "UNNEST") do
        j
      end

    {k, filtered_joins}
  end

  defp pg_traverse_final_pass({k, v}) when is_list(v) or is_map(v) do
    {k, pg_traverse_final_pass(v)}
  end

  defp pg_traverse_final_pass(kv) when is_list(kv) do
    Enum.map(kv, fn kv -> pg_traverse_final_pass(kv) end)
  end

  defp pg_traverse_final_pass(kv) when is_map(kv) do
    Enum.map(kv, fn kv -> pg_traverse_final_pass(kv) end) |> Map.new()
  end

  defp pg_traverse_final_pass(kv), do: kv

  defp bq_to_pg_field_references(ast) do
    joins = get_in(ast, ["Query", "body", "Select", "from", Access.at(0), "joins"]) || []
    cleaned_joins = Enum.filter(joins, fn join -> get_in(join, ["relation", "UNNEST"]) == nil end)

    alias_path_mappings = get_bq_alias_path_mappings(ast)

    # create mapping of cte tables to field aliases
    cte_table_names = extract_cte_aliases([ast])
    cte_tables_tree = get_in(ast, ["Query", "with", "cte_tables"])

    # TOOD: refactor
    cte_aliases =
      for table <- cte_table_names, into: %{} do
        tree =
          Enum.find(cte_tables_tree, fn tree ->
            get_in(tree, ["alias", "name", "value"]) == table
          end)

        fields =
          if tree != nil do
            for field <- get_in(tree, ["query", "body", "Select", "projection"]) || [],
                {expr, identifier} <- field,
                expr in ["UnnamedExpr", "ExprWithAlias"] do
              get_identifier_alias(identifier)
            end
          else
            []
          end

        {table, fields}
      end

    # TOOD: refactor
    cte_from_aliases =
      for table <- cte_table_names, into: %{} do
        tree =
          Enum.find(cte_tables_tree, fn tree ->
            get_in(tree, ["alias", "name", "value"]) == table
          end)

        aliases =
          if tree != nil do
            for from_tree <- get_in(tree, ["query", "body", "Select", "from"]),
                table_name = get_in(from_tree, ["relation", "Table", "alias", "name", "value"]),
                table_name != nil do
              table_name
            end
          else
            []
          end

        {table, aliases}
      end

    ast
    |> traverse_convert_identifiers(%{
      alias_path_mappings: alias_path_mappings,
      cte_aliases: cte_aliases,
      cte_from_aliases: cte_from_aliases,
      in_cte_tables_tree: false,
      in_function_or_cast: false,
      in_projection_tree: false,
      from_table_aliases: [],
      from_table_values: [],
      in_binaryop: false,
      in_between: false,
      in_inlist: false
    })
    |> then(fn
      ast when joins != [] ->
        put_in(ast, ["Query", "body", "Select", "from", Access.at(0), "joins"], cleaned_joins)

      ast ->
        ast
    end)
  end

  defp convert_keys_to_json_query(identifiers, data, base \\ "body")

  # convert body.timestamp from unix microsecond to postgres timestamp
  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => "timestamp"}]},
         %{
           in_cte_tables_tree: in_cte_tables_tree,
           cte_aliases: cte_aliases,
           in_projection_tree: false
         } = _data,
         [
           table,
           "body"
         ]
       )
       when cte_aliases == %{} or in_cte_tables_tree == true do
    at_time_zone(%{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{
            "CompoundIdentifier" => [
              %{"quote_style" => nil, "value" => table},
              %{"quote_style" => nil, "value" => "body"}
            ]
          },
          "operator" => "LongArrow",
          "right" => %{"Value" => %{"SingleQuotedString" => "timestamp"}}
        }
      }
    })
  end

  defp convert_keys_to_json_query(%{"Identifier" => %{"value" => "timestamp"}}, _data, "body") do
    at_time_zone(%{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{
            "Identifier" => %{"quote_style" => nil, "value" => "body"}
          },
          "operator" => "LongArrow",
          "right" => %{"Value" => %{"SingleQuotedString" => "timestamp"}}
        }
      }
    })
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => key}]},
         data,
         [table, field]
       ) do
    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{
            "CompoundIdentifier" => [
              %{"quote_style" => nil, "value" => table},
              %{"quote_style" => nil, "value" => field}
            ]
          },
          "operator" => json_access_arrow(data, false),
          "right" => %{"Value" => %{"SingleQuotedString" => key}}
        }
      }
    }
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => key}]},
         data,
         base
       ) do
    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "operator" => json_access_arrow(data, false),
          "right" => %{"Value" => %{"SingleQuotedString" => key}}
        }
      }
    }
  end

  # handle cross join aliases when there are different base field names as compared to what is referenced
  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => _join_alias}, %{"value" => key} | _]},
         data,
         {base, arr_path}
       ) do
    str_path = Enum.join(arr_path, ",")
    path = "{#{str_path},#{key}}"

    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "operator" => json_access_arrow(data, true),
          "right" => %{"Value" => %{"SingleQuotedString" => path}}
        }
      }
    }
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => join_alias}, %{"value" => key} | _]},
         data,
         base
       ) do
    str_path = Enum.join(data.alias_path_mappings[join_alias], ",")
    path = "{#{str_path},#{key}}"

    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "operator" => json_access_arrow(data, true),
          "right" => %{"Value" => %{"SingleQuotedString" => path}}
        }
      }
    }
  end

  defp convert_keys_to_json_query(
         %{"Identifier" => %{"value" => name}},
         data,
         base
       ) do
    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "operator" => json_access_arrow(data, false),
          "right" => %{"Value" => %{"SingleQuotedString" => name}}
        }
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

  # handle literal values
  defp get_identifier_alias(%{"expr" => _, "alias" => %{"value" => name}}) do
    name
  end

  # return non-matching as is
  defp get_identifier_alias(identifier), do: identifier

  defp get_bq_alias_path_mappings(ast) do
    from_list = get_in(ast, ["Query", "body", "Select", "from"]) || []

    table_aliases =
      Enum.map(from_list, fn from ->
        get_in(from, ["relation", "Table", "alias", "name", "value"])
      end)

    for from <- from_list do
      Enum.reduce(from["joins"] || [], %{}, fn
        %{
          "relation" => %{
            "UNNEST" => %{
              "array_expr" => %{"Identifier" => %{"value" => identifier_val}},
              "alias" => %{"name" => %{"value" => alias_name}}
            }
          }
        },
        acc ->
          Map.put(acc, alias_name, [identifier_val])

        %{
          "relation" => %{
            "UNNEST" => %{
              "array_expr" => %{"CompoundIdentifier" => identifiers},
              "alias" => %{"name" => %{"value" => alias_name}}
            }
          }
        },
        acc ->
          arr_path =
            for i <- identifiers, value = i["value"], value not in table_aliases do
              if is_map_key(acc, value), do: acc[value], else: [value]
            end
            |> List.flatten()

          Map.put(acc, alias_name, arr_path)
      end)
    end
    |> Enum.reduce(%{}, fn mappings, acc -> Map.merge(acc, mappings) end)
  end

  defp traverse_convert_identifiers({"InList" = k, v}, data) do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_inlist, true))}
  end

  defp traverse_convert_identifiers({"BinaryOp" = k, v}, data) do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_binaryop, true))}
  end

  defp traverse_convert_identifiers({"Between" = k, v}, data) do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_between, true))}
  end

  defp traverse_convert_identifiers({"cte_tables" = k, v}, data) do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_cte_tables_tree, true))}
  end

  defp traverse_convert_identifiers({"projection" = k, v}, data) do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_projection_tree, true))}
  end

  # handle top level queries
  defp traverse_convert_identifiers(
         {"Query" = k, %{"body" => %{"Select" => %{"from" => [_ | _] = from_list}}} = v},
         %{in_cte_tables_tree: false} = data
       ) do
    # TODO: refactor
    aliases =
      for from <- from_list,
          value = get_in(from, ["relation", "Table", "alias", "name", "value"]),
          value != nil do
        value
      end

    # values
    values =
      for from <- from_list,
          value_map = (get_in(from, ["relation", "Table", "name"]) || []) |> hd(),
          value_map != nil do
        value_map["value"]
      end

    alias_path_mappings = get_bq_alias_path_mappings(%{"Query" => v})

    data =
      Map.merge(data, %{
        from_table_aliases: aliases,
        from_table_values: values,
        alias_path_mappings: alias_path_mappings
      })

    {k, traverse_convert_identifiers(v, data)}
  end

  # handle CTE-level queries
  defp traverse_convert_identifiers(
         {"query" = k,
          %{
            "body" => %{
              "Select" => %{"from" => [_ | _] = from_list}
            }
          } = v},
         %{in_cte_tables_tree: true} = data
       ) do
    # TODO: refactor
    aliases =
      for from <- from_list,
          value = get_in(from, ["relation", "Table", "alias", "name", "value"]),
          value != nil do
        value
      end

    values =
      for from <- from_list,
          value_map = (get_in(from, ["relation", "Table", "name"]) || []) |> hd(),
          value_map != nil do
        value_map["value"]
      end

    alias_path_mappings = get_bq_alias_path_mappings(%{"Query" => v})

    data =
      Map.merge(data, %{
        from_table_aliases: aliases,
        from_table_values: values,
        alias_path_mappings: alias_path_mappings
      })

    {k, traverse_convert_identifiers(v, data)}
  end

  defp traverse_convert_identifiers({k, v}, data) when k in ["Function", "Cast"] do
    {k, traverse_convert_identifiers(v, Map.put(data, :in_function_or_cast, true))}
  end

  # auto set the column alias if not set
  defp traverse_convert_identifiers({"UnnamedExpr", identifier}, data)
       when is_map_key(identifier, "CompoundIdentifier") or is_map_key(identifier, "Identifier") do
    normalized_identifier = get_identifier_alias(identifier)

    if normalized_identifier do
      {"ExprWithAlias",
       %{
         "alias" => %{"quote_style" => nil, "value" => get_identifier_alias(identifier)},
         "expr" => traverse_convert_identifiers(identifier, data)
       }}
    else
      identifier
    end
  end

  defp traverse_convert_identifiers(
         {"CompoundIdentifier" = k, [%{"value" => head_val}, tail] = v},
         data
       ) do
    cond do
      # Use match?/2 there to check if list has at least 2 values in it. It is
      # faster than `langth(list) > 2` as it do not need to traverse whole list
      # during check
      is_map_key(data.alias_path_mappings, head_val) and
          match?([_, _ | _], data.alias_path_mappings[head_val || []]) ->
        # referencing a cross join unnest
        # pop first path part and use it as the base
        # with a cross join unnest(metadata) as m
        # with a cross join unnest(m.request) as request
        # reference of request.status_code gets converted to:
        # metadata -> 'request, status_code'
        # base is set to the first item of the path (full json path is metadata.request.status_code)

        # pop the first
        [base | arr_path] = data.alias_path_mappings[head_val]

        convert_keys_to_json_query(%{k => v}, data, {base, arr_path})
        |> Map.to_list()
        |> List.first()

      # outside of a cte, referencing table alias
      # preserve as is
      head_val in data.from_table_aliases and data.in_cte_tables_tree == false and
          data.cte_aliases != %{} ->
        {k, v}

      # first OR condition: outside of cte and non-cte
      # second OR condition: inside a cte
      head_val in data.from_table_aliases or
          Enum.any?(data.from_table_values, fn from ->
            head_val in Map.get(data.cte_from_aliases, from, [])
          end) ->
        # convert to t.body -> 'tail'
        convert_keys_to_json_query(%{k => [tail]}, data, [head_val, "body"])
        |> Map.to_list()
        |> List.first()

      is_map_key(data.cte_aliases, head_val) ->
        # referencing a cte field alias
        # leave as is, head.tail
        {k, v}

      Enum.any?(data.from_table_values, fn from ->
        head_val in Map.get(data.cte_aliases, from, [])
      end) ->
        # referencing a cte field, pop and convert
        # metadata.key  into metadata -> 'key'
        convert_keys_to_json_query(%{k => [tail]}, data, head_val)
        |> Map.to_list()
        |> List.first()

      true ->
        # convert to body -> '{head,tail}'
        do_normal_compount_identifier_convert({k, v}, data)
    end
  end

  # identifiers should be left as is if it is referencing a cte table
  defp traverse_convert_identifiers(
         {"Identifier" = k, %{"value" => field_alias} = v},
         %{in_cte_tables_tree: false, cte_aliases: cte_aliases} = data
       )
       when cte_aliases != %{} do
    allowed_aliases = cte_aliases |> Map.values() |> List.flatten()

    if field_alias in allowed_aliases do
      {k, v}
    else
      do_normal_compount_identifier_convert({k, v}, data)
    end
  end

  # leave compound identifier as is
  defp traverse_convert_identifiers({"CompoundIdentifier" = k, v}, _data), do: {k, v}

  defp traverse_convert_identifiers({"Identifier" = k, v}, data) do
    convert_keys_to_json_query(%{k => v}, data)
    |> Map.to_list()
    |> List.first()
  end

  defp traverse_convert_identifiers({k, v}, data) when is_list(v) or is_map(v) do
    {k, traverse_convert_identifiers(v, data)}
  end

  defp traverse_convert_identifiers(kv, data) when is_list(kv) do
    Enum.map(kv, fn kv -> traverse_convert_identifiers(kv, data) end)
  end

  defp traverse_convert_identifiers(kv, data) when is_map(kv) do
    Enum.map(kv, fn kv -> traverse_convert_identifiers(kv, data) end) |> Map.new()
  end

  defp traverse_convert_identifiers(kv, _data), do: kv

  defp do_normal_compount_identifier_convert({k, v}, data) do
    convert_keys_to_json_query(%{k => v}, data)
    |> Map.to_list()
    |> List.first()
  end

  defp identifier?(identifier),
    do: is_map_key(identifier, "CompoundIdentifier") or is_map_key(identifier, "Identifier")

  defp numeric_value?(%{"Value" => %{"Number" => _}}), do: true
  defp numeric_value?(_), do: false
  defp json_access?(%{"Nested" => %{"JsonAccess" => _}}), do: true
  defp json_access?(%{"JsonAccess" => _}), do: true
  defp json_access?(_), do: false

  defp timestamp_identifier?(%{"Identifier" => %{"value" => "timestamp"}}), do: true

  defp timestamp_identifier?(%{"CompoundIdentifier" => [_head, %{"value" => "timestamp"}]}),
    do: true

  defp timestamp_identifier?(_), do: false

  defp at_time_zone(identifier) do
    %{
      "Nested" => %{
        "AtTimeZone" => %{
          "time_zone" => "UTC",
          "timestamp" => %{
            "Function" => %{
              "args" => [
                %{
                  "Unnamed" => %{
                    "Expr" => %{
                      "BinaryOp" => %{
                        "left" => %{
                          "Cast" => %{
                            "data_type" => %{"BigInt" => nil},
                            "expr" => identifier
                          }
                        },
                        "op" => "Divide",
                        "right" => %{"Value" => %{"Number" => ["1000000.0", false]}}
                      }
                    }
                  }
                }
              ],
              "distinct" => false,
              "name" => [%{"quote_style" => nil, "value" => "to_timestamp"}],
              "over" => nil,
              "special" => false
            }
          }
        }
      }
    }
  end

  defp cast_to_numeric(expr) do
    %{
      "Cast" => %{
        "data_type" => %{"Numeric" => "None"},
        "expr" => expr
      }
    }
  end

  defp cast_to_jsonb(expr) do
    %{
      "Cast" => %{
        "data_type" => %{"Custom" => [[%{"quote_style" => nil, "value" => "jsonb"}], []]},
        "expr" => expr
      }
    }
  end

  defp jsonb_to_text(expr) do
    %{
      "Nested" => %{
        "JsonAccess" => %{
          "left" => expr,
          "operator" => "HashLongArrow",
          "right" => %{"Value" => %{"SingleQuotedString" => "{}"}}
        }
      }
    }
  end

  defp json_access_arrow(data, hash) do
    arrow =
      cond do
        data.in_binaryop or data.in_between or data.in_function_or_cast -> "LongArrow"
        true -> "Arrow"
      end

    if hash do
      "Hash" <> arrow
    else
      arrow
    end
  end
end
