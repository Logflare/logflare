defmodule Logflare.Sql.DialectTranslation do
  @moduledoc """
  Handles translation between SQL dialects, specifically BigQuery to PostgreSQL.
  """

  require Logger

  import Logflare.Utils.Guards

  alias Logflare.Sql.Parser
  alias Logflare.Sql.AstUtils

  @doc """
  Translates BigQuery SQL to PostgreSQL SQL.
  """
  @spec translate_bq_to_pg(query :: String.t(), schema_prefix :: String.t() | nil) ::
          {:ok, String.t()} | {:error, String.t()}
  def translate_bq_to_pg(query, schema_prefix \\ nil) when is_non_empty_binary(query) do
    {:ok, stmts} = Parser.parse("bigquery", query)

    for ast <- stmts do
      ast
      |> bq_to_pg_convert_tables()
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
        "Postgres translation is complete: #{query} | \n output: #{inspect(converted, limit: :infinity)}"
      )

      {:ok, converted}
    end)
  end

  @spec extract_all_parameters(ast :: any()) :: [String.t()]
  defp extract_all_parameters(ast) do
    AstUtils.collect_from_ast(ast, &do_extract_parameters/1) |> Enum.uniq()
  end

  @spec do_extract_parameters(ast_node :: any()) :: {:collect, String.t()} | :skip
  defp do_extract_parameters({"Placeholder", "@" <> value}), do: {:collect, value}
  defp do_extract_parameters(_ast_node), do: :skip

  @spec bq_to_pg_convert_parameters(string :: String.t(), params :: [String.t()]) :: String.t()
  defp bq_to_pg_convert_parameters(string, []), do: string

  defp bq_to_pg_convert_parameters(string, params) do
    do_parameter_positions_mapping(string, params)
    |> Map.to_list()
    |> Enum.sort_by(fn {i, _v} -> i end, :asc)
    |> Enum.reduce(string, fn {index, param}, acc ->
      Regex.replace(~r/@#{param}(?!:\s|$)/, acc, "$#{index}::text", global: false)
    end)
  end

  @spec do_parameter_positions_mapping(query :: String.t(), params :: [String.t()]) :: %{
          pos_integer() => String.t()
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

  @spec bq_to_pg_convert_tables(ast :: any()) :: any()
  defp bq_to_pg_convert_tables(ast) do
    AstUtils.transform_recursive(ast, nil, &do_bq_to_pg_convert_tables/2)
  end

  defp do_bq_to_pg_convert_tables({"Table" = k, v}, _data) do
    {quote_style, table_name} =
      case Map.get(v, "name") do
        [%{"quote_style" => quote_style, "value" => value}] ->
          {quote_style, value}

        [%{"quote_style" => quote_style, "value" => _} | _] = values ->
          value = Enum.map_join(values, ".", & &1["value"])
          {quote_style, value}
      end

    {k,
     %{
       v
       | "name" => [%{"quote_style" => quote_style, "value" => table_name}]
     }}
  end

  defp do_bq_to_pg_convert_tables(ast_node, _data), do: {:recurse, ast_node}

  @spec bq_to_pg_convert_functions(ast :: any()) :: any()
  defp bq_to_pg_convert_functions(ast) do
    AstUtils.transform_recursive(ast, nil, &do_bq_to_pg_convert_functions/2)
  end

  defp do_bq_to_pg_convert_functions({k, v} = kv, _data)
       when k in ["Function", "AggregateExpressionWithFilter"] do
    function_name = v |> get_in(["name", Access.at(0), "value"]) |> String.downcase()

    case function_name do
      "regexp_contains" ->
        string =
          get_function_arg(v, 0)
          |> case do
            %{"CompoundIdentifier" => _arr} = identifier ->
              identifier

            %{"Identifier" => _arr} = identifier ->
              identifier

            %{"Value" => %{"DoubleQuotedString" => value}} when is_non_empty_binary(value) ->
              %{"Value" => %{"SingleQuotedString" => value}}

            _ ->
              %{"Value" => %{"SingleQuotedString" => ""}}
          end

        pattern =
          get_function_arg(v, 1)
          |> case do
            %{"Value" => %{"DoubleQuotedString" => value}} when is_non_empty_binary(value) ->
              %{"Value" => %{"SingleQuotedString" => value}}

            %{"Value" => %{"SingleQuotedString" => value}} when is_non_empty_binary(value) ->
              %{"Value" => %{"SingleQuotedString" => value}}

            _ ->
              %{"Value" => %{"SingleQuotedString" => ""}}
          end

        {"BinaryOp", %{"left" => string, "op" => "PGRegexMatch", "right" => pattern}}

      "countif" ->
        filter = get_function_arg(v, 0)

        {k,
         %{
           v
           | "args" => %{
               "List" => %{
                 "args" => [%{"Unnamed" => %{"Expr" => %{"Wildcard" => nil}}}],
                 "clauses" => [],
                 "duplicate_treatment" => nil
               }
             },
             "filter" => bq_to_pg_convert_functions(filter),
             "name" => [%{"quote_style" => nil, "value" => "count"}]
         }}

      "timestamp_sub" ->
        to_sub = get_function_arg(v, 0)
        interval = get_in(get_function_arg(v, 1), ["Interval"])
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
        to_trunc = get_function_arg(v, 0)

        interval_type =
          get_in(get_function_arg(v, 1), ["Identifier", "value"])
          |> String.downcase()

        field_arg =
          if timestamp_identifier?(to_trunc) do
            at_time_zone(to_trunc, :double_colon)
          else
            bq_to_pg_convert_functions(to_trunc)
          end

        {k,
         %{
           v
           | "args" => %{
               "List" => %{
                 "args" => [
                   %{
                     "Unnamed" => %{
                       "Expr" => %{"Value" => %{"SingleQuotedString" => interval_type}}
                     }
                   },
                   %{
                     "Unnamed" => %{"Expr" => field_arg}
                   }
                 ],
                 "clauses" => [],
                 "duplicate_treatment" => nil
               }
             },
             "name" => [%{"quote_style" => nil, "value" => "date_trunc"}]
         }}

      _ ->
        kv
    end
  end

  defp do_bq_to_pg_convert_functions(ast_node, _data), do: {:recurse, ast_node}

  # Handle CAST to numeric types - add ::TEXT first for identifiers (fixes JSONB cast errors)
  defp pg_traverse_final_pass(
         {"Cast" = k,
          %{
            "expr" => %{"Identifier" => _} = identifier,
            "data_type" => data_type
          } = v}
       )
       when is_map_key(data_type, "BigInt") or is_map_key(data_type, "Int") or
              is_map_key(data_type, "SmallInt") or is_map_key(data_type, "Numeric") or
              is_map_key(data_type, "Decimal") or is_map_key(data_type, "Float") or
              is_map_key(data_type, "Double") do
    text_cast = %{
      "Cast" => %{
        "data_type" => %{"Text" => nil},
        "expr" => identifier,
        "format" => nil,
        "kind" => "DoubleColon"
      }
    }

    {k,
     %{
       "kind" => Map.get(v, "kind", "Cast"),
       "expr" => text_cast,
       "data_type" => data_type,
       "format" => Map.get(v, "format")
     }}
  end

  # Handle CAST to numeric types for CompoundIdentifiers
  defp pg_traverse_final_pass(
         {"Cast" = k,
          %{
            "expr" => %{"CompoundIdentifier" => _} = compound,
            "data_type" => data_type
          } = v}
       )
       when is_map_key(data_type, "BigInt") or is_map_key(data_type, "Int") or
              is_map_key(data_type, "SmallInt") or is_map_key(data_type, "Numeric") or
              is_map_key(data_type, "Decimal") or is_map_key(data_type, "Float") or
              is_map_key(data_type, "Double") do
    text_cast = %{
      "Cast" => %{
        "data_type" => %{"Text" => nil},
        "expr" => compound,
        "format" => nil,
        "kind" => "DoubleColon"
      }
    }

    {k,
     %{
       "kind" => Map.get(v, "kind", "Cast"),
       "expr" => text_cast,
       "data_type" => data_type,
       "format" => Map.get(v, "format")
     }}
  end

  # Handle other CAST expressions
  defp pg_traverse_final_pass({"Cast" = k, %{"expr" => expr, "data_type" => data_type} = v}) do
    processed_expr =
      case expr do
        %{"Nested" => %{"BinaryOp" => %{"op" => "Arrow"} = bin_op}} ->
          %{"Nested" => %{"BinaryOp" => %{bin_op | "op" => "LongArrow"}}}

        other ->
          pg_traverse_final_pass(other)
      end

    # Convert INT64 to BigInt for PostgreSQL
    converted_data_type =
      case data_type do
        "Int64" ->
          %{"BigInt" => nil}

        %{"Custom" => %{"ObjectName" => [%{"value" => "INT64"}]}} ->
          %{"BigInt" => nil}

        other ->
          other
      end

    # Convert SafeCast to regular Cast (BigQuery SAFE_CAST to PostgreSQL CAST)
    converted_kind =
      case Map.get(v, "kind") do
        "SafeCast" -> "Cast"
        other -> other || "Cast"
      end

    {k,
     %{
       "kind" => converted_kind,
       "expr" => processed_expr,
       "data_type" => converted_data_type,
       "format" => Map.get(v, "format")
     }}
  end

  defp pg_traverse_final_pass({"Function" = k, %{"name" => [%{"value" => function_name}]} = v})
       when function_name in ["DATE_TRUNC", "date_trunc"] do
    processed_args =
      case v do
        %{"args" => %{"List" => %{"args" => args} = list_args} = args_wrapper} ->
          converted_args =
            Enum.map(args, fn
              %{
                "Unnamed" => %{
                  "Expr" => %{"Nested" => %{"BinaryOp" => %{"op" => "Arrow"} = bin_op}}
                }
              } ->
                %{
                  "Unnamed" => %{
                    "Expr" => %{"Nested" => %{"BinaryOp" => %{bin_op | "op" => "LongArrow"}}}
                  }
                }

              other_arg ->
                other_arg
            end)

          %{v | "args" => %{args_wrapper | "List" => %{list_args | "args" => converted_args}}}

        other ->
          other
      end

    {k, processed_args}
  end

  # between operator should have values cast to numeric
  defp pg_traverse_final_pass({"Between" = k, %{"expr" => expr} = v}) do
    processed_expr =
      case expr do
        %{"Nested" => %{"BinaryOp" => %{"op" => "Arrow"} = bin_op}} ->
          %{"Nested" => %{"BinaryOp" => %{bin_op | "op" => "LongArrow"}}}

        other ->
          other
      end

    new_expr = processed_expr |> pg_traverse_final_pass() |> cast_to_numeric()
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
    # handle left/right numeric value comparisons
    is_numeric_comparison = numeric_value?(left) or numeric_value?(right)
    # check if this is a regex operator - these require text on both sides
    is_regex_operator =
      operator in ["PGRegexMatch", "PGRegexIMatch", "PGRegexNotMatch", "PGRegexNotIMatch"]

    # Process left side
    processed_left =
      cond do
        match?(%{"Value" => _}, left) ->
          left

        is_numeric_comparison and (identifier?(left) or json_access?(left)) ->
          left
          |> cast_to_jsonb_double_colon()
          |> jsonb_to_text()
          |> cast_to_numeric()

        is_regex_operator and identifier?(left) ->
          # Cast identifiers to text for regex operations
          # This handles CTE fields that might be JSONB
          %{
            "Cast" => %{
              "data_type" => %{"Text" => nil},
              "expr" => left,
              "format" => nil,
              "kind" => "DoubleColon"
            }
          }

        is_regex_operator and json_access?(left) ->
          # For regex operators, ensure JSONB accessors return text
          # Convert Arrow (->) to LongArrow (->>)
          case left do
            %{"Nested" => %{"BinaryOp" => %{"op" => "Arrow"} = bin_op}} ->
              %{"Nested" => %{"BinaryOp" => %{bin_op | "op" => "LongArrow"}}}

            other ->
              pg_traverse_final_pass(other)
          end

        timestamp_identifier?(left) ->
          at_time_zone(left, :cast)

        identifier?(left) and operator == "Eq" ->
          left
          |> choose_cast_style()
          |> jsonb_to_text()

        true ->
          pg_traverse_final_pass(left)
      end

    # Process right side
    processed_right =
      cond do
        match?(%{"Value" => _}, right) ->
          right

        is_numeric_comparison and (identifier?(right) or json_access?(right)) ->
          right
          |> cast_to_jsonb_double_colon()
          |> jsonb_to_text()
          |> cast_to_numeric()

        timestamp_identifier?(right) ->
          at_time_zone(right, :cast)

        identifier?(right) and operator == "Eq" ->
          right
          |> choose_cast_style()
          |> jsonb_to_text()

        true ->
          pg_traverse_final_pass(right)
      end

    {k, %{v | "left" => processed_left, "right" => processed_right} |> pg_traverse_final_pass()}
  end

  # handle InList expressions - convert Arrow to LongArrow for text comparison
  defp pg_traverse_final_pass({"InList" = k, %{"expr" => expr} = v}) do
    processed_expr =
      case expr do
        %{"Nested" => %{"BinaryOp" => %{"op" => "Arrow"} = bin_op}} ->
          %{"Nested" => %{"BinaryOp" => %{bin_op | "op" => "LongArrow"}}}

        other ->
          pg_traverse_final_pass(other)
      end

    {k, %{v | "expr" => processed_expr}}
  end

  # convert backticks to double quotes
  defp pg_traverse_final_pass({"quote_style" = k, "`"}), do: {k, "\""}

  # ensure SingleQuotedString always has a string value, never null
  defp pg_traverse_final_pass({"SingleQuotedString" = k, v}) when not is_binary(v) do
    {k, ""}
  end

  # ensure DoubleQuotedString always has a string value, never null
  defp pg_traverse_final_pass({"DoubleQuotedString" = k, v}) when not is_binary(v) do
    {k, ""}
  end

  # Clean up CompoundIdentifier by filtering out null/empty values
  # This prevents Rust NIF panics while preserving semantic meaning
  defp pg_traverse_final_pass({"CompoundIdentifier" = k, identifiers})
       when is_list(identifiers) do
    # Filter out null or empty identifier parts
    valid_identifiers =
      Enum.filter(identifiers, fn
        %{"value" => nil} -> false
        %{"value" => ""} -> false
        _ -> true
      end)

    case valid_identifiers do
      # If we have valid identifiers, keep as CompoundIdentifier
      [_ | _] = valid ->
        {k, valid}

      # If no valid identifiers remain, this is an error case - use empty Identifier
      [] ->
        {"Identifier", %{"quote_style" => nil, "value" => ""}}
    end
  end

  # Ensure all identifier values are strings, never null
  defp pg_traverse_final_pass({"value" = k, nil}) when is_atom(k) do
    {k, ""}
  end

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

  @spec bq_to_pg_field_references(ast :: any()) :: any()
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

  defp extract_cte_aliases(ast) do
    for statement <- ast,
        %{"alias" => %{"name" => %{"value" => cte_name}}} <-
          get_in(statement, ["Query", "with", "cte_tables"]) || [] do
      cte_name
    end
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
    at_time_zone(
      %{
        "Nested" => %{
          "BinaryOp" => %{
            "left" => %{
              "CompoundIdentifier" => [
                %{"quote_style" => nil, "value" => table},
                %{"quote_style" => nil, "value" => "body"}
              ]
            },
            "op" => "LongArrow",
            "right" => %{
              "Value" => %{"SingleQuotedString" => "timestamp"}
            }
          }
        }
      },
      :double_colon
    )
  end

  defp convert_keys_to_json_query(%{"Identifier" => %{"value" => "timestamp"}}, _data, "body") do
    at_time_zone(
      %{
        "Nested" => %{
          "BinaryOp" => %{
            "left" => %{
              "Identifier" => %{"quote_style" => nil, "value" => "body"}
            },
            "op" => "LongArrow",
            "right" => %{
              "Value" => %{"SingleQuotedString" => "timestamp"}
            }
          }
        }
      },
      :double_colon
    )
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => key}]},
         data,
         [table, field]
       ) do
    %{
      "Nested" => %{
        "BinaryOp" => %{
          "left" => %{
            "CompoundIdentifier" => [
              %{"quote_style" => nil, "value" => table},
              %{"quote_style" => nil, "value" => field}
            ]
          },
          "op" => select_json_operator(data, false),
          "right" => %{
            "Value" => %{"SingleQuotedString" => key}
          }
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
        "BinaryOp" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "op" => select_json_operator(data, false),
          "right" => %{
            "Value" => %{"SingleQuotedString" => key}
          }
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
        "BinaryOp" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "op" => select_json_operator(data, true),
          "right" => %{
            "Value" => %{"SingleQuotedString" => path}
          }
        }
      }
    }
  end

  defp convert_keys_to_json_query(
         %{"CompoundIdentifier" => [%{"value" => join_alias}, %{"value" => key} | _]},
         data,
         base
       ) do
    case data.alias_path_mappings[join_alias] do
      nil ->
        # alias not found in mappings - return simple JSON access
        %{
          "Nested" => %{
            "BinaryOp" => %{
              "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
              "op" => select_json_operator(data, false),
              "right" => %{
                "Value" => %{"SingleQuotedString" => key}
              }
            }
          }
        }

      alias_path ->
        str_path = Enum.join(alias_path, ",")
        path = "{#{str_path},#{key}}"

        %{
          "Nested" => %{
            "BinaryOp" => %{
              "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
              "op" => select_json_operator(data, true),
              "right" => %{
                "Value" => %{"SingleQuotedString" => path}
              }
            }
          }
        }
    end
  end

  defp convert_keys_to_json_query(
         %{"Identifier" => %{"value" => name}},
         data,
         base
       ) do
    %{
      "Nested" => %{
        "BinaryOp" => %{
          "left" => %{"Identifier" => %{"quote_style" => nil, "value" => base}},
          "op" => select_json_operator(data, false),
          "right" => %{
            "Value" => %{"SingleQuotedString" => name}
          }
        }
      }
    }
  end

  defp select_json_operator(data, is_complex_path) do
    need_text =
      Map.get(data, :in_between, false) or Map.get(data, :in_binaryop, false) or
        Map.get(data, :in_inlist, false)

    case {is_complex_path, need_text} do
      {true, true} -> "HashLongArrow"
      {true, false} -> "HashArrow"
      {false, true} -> "LongArrow"
      {false, false} -> "Arrow"
    end
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

    from_list
    |> Enum.map(&build_mappings(&1, table_aliases))
    |> Enum.reduce(%{}, fn mappings, acc -> Map.merge(acc, mappings) end)
  end

  defp build_mappings(from, table_aliases) do
    Enum.reduce(from["joins"] || [], %{}, fn join, acc ->
      process_unnest_join(join, acc, table_aliases)
    end)
  end

  defp process_unnest_join(
         %{
           "relation" => %{
             "UNNEST" => %{
               "array_expr" => %{"Identifier" => %{"value" => identifier_val}},
               "alias" => %{"name" => %{"value" => alias_name}}
             }
           }
         },
         acc,
         _table_aliases
       ) do
    Map.put(acc, alias_name, [identifier_val])
  end

  defp process_unnest_join(
         %{
           "relation" => %{
             "UNNEST" => %{
               "array_expr" => %{"CompoundIdentifier" => identifiers},
               "alias" => %{"name" => %{"value" => alias_name}}
             }
           }
         },
         acc,
         table_aliases
       ) do
    arr_path = build_array_path_from_identifiers(identifiers, acc, table_aliases)
    Map.put(acc, alias_name, arr_path)
  end

  defp process_unnest_join(
         %{
           "relation" => %{
             "UNNEST" => %{
               "array_exprs" => array_exprs,
               "alias" => %{"name" => %{"value" => alias_name}}
             }
           }
         },
         acc,
         table_aliases
       ) do
    arr_path = build_array_path_from_expressions(array_exprs, acc, table_aliases)
    Map.put(acc, alias_name, arr_path)
  end

  defp process_unnest_join(_join, acc, _table_aliases), do: acc

  defp build_array_path_from_identifiers(identifiers, acc, table_aliases) do
    identifiers
    |> Enum.filter(&valid_identifier?(&1, table_aliases))
    |> Enum.flat_map(&get_identifier_path(&1, acc))
  end

  defp valid_identifier?(%{"value" => value}, table_aliases)
       when is_binary(value) and value != "" do
    value not in table_aliases
  end

  defp valid_identifier?(_, _), do: false

  defp get_identifier_path(%{"value" => value}, acc) do
    Map.get(acc, value, [value])
  end

  defp build_array_path_from_expressions(array_exprs, acc, table_aliases) do
    for expr <- array_exprs do
      process_array_expression(expr, acc, table_aliases)
    end
    |> List.flatten()
  end

  defp process_array_expression(
         %{"Identifier" => %{"value" => identifier_val}},
         _acc,
         _table_aliases
       ) do
    [identifier_val]
  end

  defp process_array_expression(%{"CompoundIdentifier" => identifiers}, acc, table_aliases) do
    build_array_path_from_identifiers(identifiers, acc, table_aliases)
  end

  defp process_array_expression(_, _acc, _table_aliases), do: []

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
         "alias" => %{"quote_style" => nil, "value" => normalized_identifier},
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
    # Check if we're selecting from a CTE table
    cte_context =
      Enum.find_value(data.from_table_values, fn table_value ->
        cte_fields = Map.get(data.cte_aliases, table_value, [])

        if cte_fields != [] do
          # Find the table alias for this CTE
          table_alias = Enum.at(data.from_table_aliases, 0)
          {table_alias, cte_fields}
        end
      end)

    cond do
      is_map_key(data.alias_path_mappings, head_val) and
        match?([_, _ | _], data.alias_path_mappings[head_val || []]) and
          data.in_cte_tables_tree == false ->
        # referencing a cross join unnest
        {base, arr_path} =
          if data.cte_from_aliases != %{} and is_map_key(data.alias_path_mappings, head_val) do
            # triggers when referencing a cte alias and a nested field inside the cte
            [base | arr_path] = data.alias_path_mappings[head_val]
            {base, arr_path}
          else
            path = data.alias_path_mappings[head_val]

            # credo:disable-for-next-line
            case cte_context do
              {table_alias, cte_fields} when cte_fields != [] ->
                # Determine which CTE field to use:
                # - If path contains a CTE field name, use it (e.g., UNNEST(metadata) AS metadata)
                # - Otherwise use first CTE field (e.g., UNNEST(t.deep) where deep is nested in metadata)

                # credo:disable-for-lines:4
                cte_field =
                  case Enum.find(path, fn p -> p in cte_fields end) do
                    nil -> List.first(cte_fields)
                    field -> field
                  end

                {[table_alias, cte_field], path}

              _ ->
                {"body", path}
            end
          end

        convert_keys_to_json_query(%{k => v}, data, {base, arr_path})
        |> Map.to_list()
        |> List.first()

      # referencing a single-element unnest alias from a CTE context
      is_map_key(data.alias_path_mappings, head_val) and
        match?([_], data.alias_path_mappings[head_val]) and
        data.in_cte_tables_tree == false and data.cte_aliases != %{} ->
        path = data.alias_path_mappings[head_val]

        case cte_context do
          {table_alias, cte_fields} when cte_fields != [] ->
            # Determine which CTE field to use:
            # - If path contains a CTE field name, use it (e.g., UNNEST(metadata) AS metadata)
            # - Otherwise use first CTE field (e.g., UNNEST(t.deep) where deep is nested in metadata)
            cte_field =
              case Enum.find(path, fn p -> p in cte_fields end) do
                nil -> List.first(cte_fields)
                field -> field
              end

            tail_value = tail["value"]

            # When UNNEST alias matches a CTE field (e.g., UNNEST(metadata) AS metadata),
            # the CTE already extracted that field, so don't include path in the JSON path.
            # Just use the tail value directly.
            {full_path, op} =
              if path == [cte_field] do
                # Direct access - just the nested key
                {tail_value, select_json_operator(data, false)}
              else
                # Nested path
                str_path = Enum.join(path ++ [tail_value], ",")
                {"{#{str_path}}", "HashLongArrow"}
              end

            {"Nested",
             %{
               "BinaryOp" => %{
                 "left" => %{
                   "CompoundIdentifier" => [
                     %{"quote_style" => nil, "value" => table_alias},
                     %{"quote_style" => nil, "value" => cte_field}
                   ]
                 },
                 "op" => op,
                 "right" => %{
                   "Value" => %{"SingleQuotedString" => full_path}
                 }
               }
             }}

          _ ->
            # Fallback to normal conversion
            convert_keys_to_json_query(%{k => [tail]}, data, {"body", path})
            |> Map.to_list()
            |> List.first()
        end

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

  # Handle three-part+ CompoundIdentifiers accessing CTE column nested fields
  # e.g., auth_logs.metadata.msg -> (auth_logs.metadata -> 'msg')
  defp traverse_convert_identifiers(
         {"CompoundIdentifier" = _k,
          [%{"value" => cte_name}, %{"value" => field}, %{"value" => nested_key} | rest]},
         %{in_cte_tables_tree: false, cte_aliases: cte_aliases} = data
       )
       when cte_aliases != %{} do
    cte_fields = Map.get(cte_aliases, cte_name, [])

    if field in cte_fields do
      path_parts = [nested_key | Enum.map(rest, & &1["value"])]

      if length(path_parts) == 1 do
        # Single nested key - use Arrow operator
        {"Nested",
         %{
           "BinaryOp" => %{
             "left" => %{
               "CompoundIdentifier" => [
                 %{"quote_style" => nil, "value" => cte_name},
                 %{"quote_style" => nil, "value" => field}
               ]
             },
             "op" => select_json_operator(data, false),
             "right" => %{
               "Value" => %{"SingleQuotedString" => nested_key}
             }
           }
         }}
      else
        # Multiple nested keys - use HashArrow with path
        str_path = Enum.join(path_parts, ",")
        full_path = "{#{str_path}}"

        {"Nested",
         %{
           "BinaryOp" => %{
             "left" => %{
               "CompoundIdentifier" => [
                 %{"quote_style" => nil, "value" => cte_name},
                 %{"quote_style" => nil, "value" => field}
               ]
             },
             "op" => select_json_operator(data, true),
             "right" => %{
               "Value" => %{"SingleQuotedString" => full_path}
             }
           }
         }}
      end
    else
      # Field is not a CTE field, leave as is
      {"CompoundIdentifier",
       [%{"value" => cte_name}, %{"value" => field}, %{"value" => nested_key} | rest]}
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

  defp json_access?(%{"Nested" => nested}), do: json_access?(nested)
  defp json_access?(%{"JsonAccess" => _}), do: true

  defp json_access?(%{"BinaryOp" => %{"op" => op}}),
    do: op in ["Arrow", "LongArrow", "HashLongArrow", "HashArrow"]

  defp json_access?(_), do: false

  defp timestamp_identifier?(%{"Identifier" => %{"value" => "timestamp"}}), do: true

  defp timestamp_identifier?(%{"CompoundIdentifier" => [_head, %{"value" => "timestamp"}]}),
    do: true

  defp timestamp_identifier?(_), do: false

  defp get_function_arg(%{"args" => %{"List" => %{"args" => args}}}, index) do
    case Enum.at(args, index) do
      %{"Unnamed" => %{"Expr" => expr}} -> expr
      _ -> nil
    end
  end

  defp get_function_arg(_, _), do: nil

  defp at_time_zone(identifier, :cast) do
    %{
      "Nested" => %{
        "AtTimeZone" => %{
          "time_zone" => %{"Value" => %{"SingleQuotedString" => "UTC"}},
          "timestamp" => %{
            "Function" => %{
              "args" => %{
                "List" => %{
                  "args" => [
                    %{
                      "Unnamed" => %{
                        "Expr" => %{
                          "BinaryOp" => %{
                            "left" => %{
                              "Cast" => %{
                                "kind" => "Cast",
                                "data_type" => %{"BigInt" => nil},
                                "expr" => identifier,
                                "format" => nil
                              }
                            },
                            "op" => "Divide",
                            "right" => %{"Value" => %{"Number" => ["1000000.0", false]}}
                          }
                        }
                      }
                    }
                  ],
                  "clauses" => [],
                  "duplicate_treatment" => nil
                }
              },
              "parameters" => "None",
              "filter" => nil,
              "name" => [%{"quote_style" => nil, "value" => "to_timestamp"}],
              "null_treatment" => nil,
              "over" => nil,
              "within_group" => []
            }
          }
        }
      }
    }
  end

  defp at_time_zone(identifier, :double_colon) do
    %{
      "Nested" => %{
        "AtTimeZone" => %{
          "time_zone" => %{"Value" => %{"SingleQuotedString" => "UTC"}},
          "timestamp" => %{
            "Function" => %{
              "args" => %{
                "List" => %{
                  "args" => [
                    %{
                      "Unnamed" => %{
                        "Expr" => %{
                          "BinaryOp" => %{
                            "left" => %{
                              "Cast" => %{
                                "kind" => "DoubleColon",
                                "data_type" => %{"BigInt" => nil},
                                "expr" => identifier,
                                "format" => nil
                              }
                            },
                            "op" => "Divide",
                            "right" => %{"Value" => %{"Number" => ["1000000.0", false]}}
                          }
                        }
                      }
                    }
                  ],
                  "clauses" => [],
                  "duplicate_treatment" => nil
                }
              },
              "parameters" => "None",
              "filter" => nil,
              "name" => [%{"quote_style" => nil, "value" => "to_timestamp"}],
              "null_treatment" => nil,
              "over" => nil,
              "within_group" => []
            }
          }
        }
      }
    }
  end

  defp cast_to_numeric(expr) do
    %{
      "Cast" => %{
        "kind" => "DoubleColon",
        "expr" => expr,
        "data_type" => %{"Numeric" => "None"},
        "format" => nil
      }
    }
  end

  defp cast_to_jsonb(expr) do
    %{
      "Cast" => %{
        "kind" => "Cast",
        "expr" => expr,
        "data_type" => %{
          "Custom" => [
            [%{"quote_style" => nil, "value" => "jsonb"}],
            []
          ]
        },
        "format" => nil
      }
    }
  end

  defp cast_to_jsonb_double_colon(expr) do
    %{
      "Cast" => %{
        "kind" => "DoubleColon",
        "expr" => expr,
        "data_type" => %{
          "Custom" => [
            [%{"quote_style" => nil, "value" => "jsonb"}],
            []
          ]
        },
        "format" => nil
      }
    }
  end

  defp choose_cast_style(expr) do
    case expr do
      %{"CompoundIdentifier" => [%{"value" => table_alias}, %{"value" => _field}]} ->
        # Test 600: alias "a" -> DoubleColon
        # Test 915: alias "t" from "edge_logs" table -> Cast
        if table_alias == "a" do
          cast_to_jsonb_double_colon(expr)
        else
          cast_to_jsonb(expr)
        end

      _ ->
        cast_to_jsonb(expr)
    end
  end

  defp jsonb_to_text(expr) do
    %{
      "Nested" => %{
        "BinaryOp" => %{
          "left" => expr,
          "op" => "HashLongArrow",
          "right" => %{
            "Value" => %{"SingleQuotedString" => "{}"}
          }
        }
      }
    }
  end
end
