defmodule Logflare.CredoChecks.ReplicatedExecuteScope do
  @moduledoc """
  Shared AST helper for the migration replication checks: locates the line ranges
  covered by `Logflare.Repo.Migrator.with_replicated_execute/1` blocks.
  """

  @wrapper_fun :with_replicated_execute

  @spec line_ranges(Macro.t()) :: [Range.t()]
  def line_ranges(ast) do
    ast
    |> Macro.prewalk([], fn node, acc ->
      if wrapper_call?(node), do: {node, [subtree_range(node) | acc]}, else: {node, acc}
    end)
    |> elem(1)
    |> Enum.reject(&is_nil/1)
  end

  @spec within?([Range.t()], pos_integer() | nil) :: boolean()
  def within?(_ranges, nil), do: false
  def within?(ranges, line), do: Enum.any?(ranges, &(line in &1))

  defp wrapper_call?({{:., _, [_module, @wrapper_fun]}, _meta, _args}), do: true
  defp wrapper_call?({@wrapper_fun, _meta, args}) when is_list(args), do: true
  defp wrapper_call?(_node), do: false

  defp subtree_range(node) do
    lines =
      node
      |> Macro.prewalk([], fn
        {_form, meta, _args} = child, acc when is_list(meta) ->
          {child, [meta[:line], meta[:closing][:line], meta[:end][:line] | acc]}

        child, acc ->
          {child, acc}
      end)
      |> elem(1)
      |> Enum.reject(&is_nil/1)

    case lines do
      [] -> nil
      lines -> Enum.min(lines)..Enum.max(lines)
    end
  end
end
