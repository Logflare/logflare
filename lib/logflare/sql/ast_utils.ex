defmodule Logflare.Sql.AstUtils do
  @moduledoc """
  Utilities for traversing and transforming SQL ASTs.
  """

  import Logflare.Utils.Guards

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
