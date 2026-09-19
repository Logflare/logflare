defmodule Logflare.Sql.AstUtils do
  @moduledoc """
  Utilities for traversing and transforming SQL ASTs.
  """

  import Logflare.Utils.Guards

  @empty_span %{
    "start" => %{"line" => 0, "column" => 0},
    "end" => %{"line" => 0, "column" => 0}
  }

  @doc """
  Builds an identifier AST node map, including the `span` field the parser
  requires when a hand-built AST is serialized back into SQL.
  """
  @spec build_identifier(value :: String.t() | nil, quote_style :: String.t() | nil) :: map()
  def build_identifier(value, quote_style \\ nil) do
    %{"value" => value, "quote_style" => quote_style, "span" => @empty_span}
  end

  @doc """
  Builds an object-name path segment (`ObjectNamePart::Identifier`), the element
  type of table and function `name` lists.
  """
  @spec build_object_name_part(value :: String.t(), quote_style :: String.t() | nil) :: map()
  def build_object_name_part(value, quote_style \\ nil) do
    %{"Identifier" => build_identifier(value, quote_style)}
  end

  @doc """
  Builds a literal value AST node (`Expr::Value`), including the `span` field the
  parser requires when a hand-built AST is serialized back into SQL.
  """
  @spec build_value(value :: map()) :: map()
  def build_value(value) when is_map(value) do
    %{"Value" => %{"value" => value, "span" => @empty_span}}
  end

  @doc """
  Builds a `CAST` expression node with the fields the parser requires when a
  hand-built AST is serialized back into SQL.
  """
  @spec build_cast(expr :: map(), data_type :: map() | String.t(), kind :: String.t()) :: map()
  def build_cast(expr, data_type, kind \\ "Cast") when is_map(expr) do
    %{
      "Cast" => %{
        "kind" => kind,
        "expr" => expr,
        "data_type" => data_type,
        "array" => false,
        "format" => nil
      }
    }
  end

  @doc """
  Returns the identifier value of a single object-name path segment.
  """
  @spec object_name_part_value(part :: map()) :: String.t()
  def object_name_part_value(%{"Identifier" => %{"value" => value}}), do: value

  @doc """
  Returns the identifier values of an object name's path segments, in order.
  """
  @spec object_name_values(parts :: [map()]) :: [String.t()]
  def object_name_values(parts) when is_list(parts),
    do: Enum.map(parts, &object_name_part_value/1)

  @doc """
  Recursively transforms an AST using a provided transform function.

  Transform function should return `{:recurse, node}` to continue traversal.
  Any other value will be cause the traversal to end.
  """
  @spec transform_recursive(ast_node :: any(), data :: any(), transform_fn :: function()) :: any()
  def transform_recursive(ast_node, data, transform_fn) when is_function(transform_fn) do
    case transform_fn.(ast_node, data) do
      {:recurse, node} -> do_recursive_transform(node, data, transform_fn)
      result -> result
    end
  end

  defp do_recursive_transform({k, v}, data, transform_fn) when is_list_or_map(v) do
    {k, transform_recursive(v, data, transform_fn)}
  end

  defp do_recursive_transform(ast_list, data, transform_fn) when is_list(ast_list) do
    Enum.map(ast_list, fn node -> transform_recursive(node, data, transform_fn) end)
  end

  defp do_recursive_transform(ast_map, data, transform_fn) when is_map(ast_map) do
    Enum.map(ast_map, fn kv -> transform_recursive(kv, data, transform_fn) end) |> Map.new()
  end

  defp do_recursive_transform(ast_node, _data, _transform_fn), do: ast_node

  @doc """
  Collects the names of `@name` query parameters, without the prefix and in order
  of first appearance.

  Most dialects tokenize `@name` as a `Placeholder` value; BigQuery tokenizes it
  as an `Identifier`. BigQuery `@@name` system variables are not parameters.
  """
  @spec extract_parameters(ast :: any()) :: [String.t()]
  def extract_parameters(ast) do
    ast |> collect_from_ast(&do_extract_parameter/1) |> Enum.uniq()
  end

  defp do_extract_parameter({"Placeholder", "@" <> name}), do: {:collect, name}
  defp do_extract_parameter({"Identifier", %{"value" => "@@" <> _}}), do: :skip
  defp do_extract_parameter({"Identifier", %{"value" => "@" <> name}}), do: {:collect, name}
  defp do_extract_parameter(_ast_node), do: :skip

  @doc """
  Collects items from an AST using a provided collector function.

  The collector function should return either:
  - `{:collect, item}` to add item to the result list and stop recursing on this node
  - `:skip` to continue recursing without collecting from this node
  """
  @spec collect_from_ast(ast :: any(), collector_fn :: function()) :: list()
  def collect_from_ast(ast, collector_fn) when is_function(collector_fn) do
    do_collect_from_ast(ast, [], collector_fn) |> Enum.reverse()
  end

  defp do_collect_from_ast(ast_node, acc, collector_fn) do
    case collector_fn.(ast_node) do
      {:collect, item} ->
        new_acc = [item | acc]
        do_recursive_collect(ast_node, new_acc, collector_fn)

      :skip ->
        do_recursive_collect(ast_node, acc, collector_fn)
    end
  end

  defp do_recursive_collect({_k, v}, acc, collector_fn) when is_list_or_map(v) do
    do_collect_from_ast(v, acc, collector_fn)
  end

  defp do_recursive_collect(ast_list, acc, collector_fn) when is_list(ast_list) do
    Enum.reduce(ast_list, acc, fn node, current_acc ->
      do_collect_from_ast(node, current_acc, collector_fn)
    end)
  end

  defp do_recursive_collect(ast_map, acc, collector_fn) when is_map(ast_map) do
    Enum.reduce(ast_map, acc, fn kv, current_acc ->
      do_collect_from_ast(kv, current_acc, collector_fn)
    end)
  end

  defp do_recursive_collect(_ast_node, acc, _collector_fn), do: acc
end
