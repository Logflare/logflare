defmodule Logflare.CredoChecks.ModuleAliases do
  @moduledoc """
  Shared AST helper for the migration replication checks: applies `alias` declarations
  (and `require ..., as:`) to an alias map, so a call through a shortened or renamed
  alias can be matched against its full module.
  """

  @type t :: %{atom() => [atom()]}

  @spec declare(Macro.t(), t()) :: t()
  def declare({:alias, _meta, [target | opts]}, aliases),
    do: Map.merge(aliases, declared(target, opts, aliases))

  def declare({:require, _meta, [target, opts]}, aliases) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.has_key?(opts, :as),
      do: Map.merge(aliases, declared(target, [opts], aliases)),
      else: aliases
  end

  def declare(_node, aliases), do: aliases

  @spec resolve(list(), t()) :: list()
  def resolve([first | rest] = segments, aliases) do
    case Map.fetch(aliases, first) do
      {:ok, full} -> full ++ rest
      :error -> segments
    end
  end

  def resolve(segments, _aliases), do: segments

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
