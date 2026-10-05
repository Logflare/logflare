defmodule Logflare.CredoChecks.ReplicatedExecuteScope do
  @moduledoc """
  Shared AST helper for the migration replication checks: locates the line ranges
  covered by `Logflare.Repo.Migrator.with_replicated_execute/1` blocks.

  Calls are only recognised through `Logflare.Repo.Migrator` itself, one of its aliases,
  or a bare call when the module is imported.
  """

  alias Logflare.CredoChecks.ModuleAliases

  @wrapper_module [:Logflare, :Repo, :Migrator]
  @wrapper_fun :with_replicated_execute

  @spec line_ranges(Macro.t()) :: [Range.t()]
  def line_ranges(ast) do
    aliases = ModuleAliases.collect(ast)
    imported? = @wrapper_module in ModuleAliases.imported_modules(ast, aliases)

    ast
    |> Macro.prewalk([], fn node, acc ->
      if wrapper_call?(node, aliases, imported?),
        do: {node, [subtree_range(node) | acc]},
        else: {node, acc}
    end)
    |> elem(1)
    |> Enum.reject(&is_nil/1)
  end

  @spec within?([Range.t()], pos_integer() | nil) :: boolean()
  def within?(_ranges, nil), do: false
  def within?(ranges, line), do: Enum.any?(ranges, &(line in &1))

  defp wrapper_call?(
         {{:., _, [{:__aliases__, _, segments}, @wrapper_fun]}, _meta, _args},
         aliases,
         _imported?
       ),
       do: ModuleAliases.resolve(segments, aliases) == @wrapper_module

  defp wrapper_call?({@wrapper_fun, _meta, args}, _aliases, imported?) when is_list(args),
    do: imported?

  defp wrapper_call?(_node, _aliases, _imported?), do: false

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
