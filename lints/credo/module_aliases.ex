defmodule Logflare.CredoChecks.ModuleAliases do
  @moduledoc """
  Shared AST helper for the migration replication checks: resolves `alias` declarations
  so a call through a shortened or renamed alias can be matched against its full module.
  """

  @type t :: %{atom() => [atom()]}

  @spec collect(Macro.t()) :: t()
  def collect(ast) do
    ast
    |> Macro.prewalk(%{}, fn
      {:alias, _meta, [target | opts]} = node, aliases ->
        {node, Map.merge(aliases, declared(target, opts, aliases))}

      node, aliases ->
        {node, aliases}
    end)
    |> elem(1)
  end

  @spec resolve(list(), t()) :: list()
  def resolve([first | rest] = segments, aliases) do
    case Map.fetch(aliases, first) do
      {:ok, full} -> full ++ rest
      :error -> segments
    end
  end

  def resolve(segments, _aliases), do: segments

  @spec imported_modules(Macro.t(), t()) :: [[atom()]]
  def imported_modules(ast, aliases) do
    ast
    |> Macro.prewalk([], fn
      {:import, _meta, [{:__aliases__, _, segments} | _opts]} = node, modules ->
        {node, [resolve(segments, aliases) | modules]}

      node, modules ->
        {node, modules}
    end)
    |> elem(1)
  end

  defp declared({:__aliases__, _, segments}, [opts], aliases) when is_list(opts) do
    case Keyword.get(opts, :as) do
      {:__aliases__, _, [as]} -> %{as => resolve(segments, aliases)}
      _ -> declared({:__aliases__, [], segments}, [], aliases)
    end
  end

  defp declared({:__aliases__, _, segments}, [], aliases) do
    %{List.last(segments) => resolve(segments, aliases)}
  end

  defp declared({{:., _, [{:__aliases__, _, base}, :{}]}, _, children}, [], aliases) do
    base = resolve(base, aliases)

    for {:__aliases__, _, child} <- children, into: %{} do
      {List.last(child), base ++ child}
    end
  end

  defp declared(_target, _opts, _aliases), do: %{}
end
