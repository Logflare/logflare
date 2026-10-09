defmodule Logflare.CredoChecks.ReplicatedExecuteScope do
  @moduledoc """
  Shared AST walker for the migration replication checks. Every node is visited with the
  lexical environment in effect at that point: the aliases declared so far, whether
  `Logflare.Repo.Migrator.with_replicated_execute/1` is imported, and whether the node is
  inside a `with_replicated_execute/1` call.

  As in Elixir, `alias` and `import` only apply to the expressions after them in the same
  block, and declarations inside a nested block (a function body, `fn`, `if`, ...) do not
  leak out of it.
  """

  alias Logflare.CredoChecks.ModuleAliases

  @wrapper_module [:Logflare, :Repo, :Migrator]
  @wrapper_fun :with_replicated_execute

  defstruct aliases: %{}, wrapper_imported?: false, replicated?: false

  @type t :: %__MODULE__{
          aliases: ModuleAliases.t(),
          wrapper_imported?: boolean(),
          replicated?: boolean()
        }

  @spec walk(Macro.t(), acc, (Macro.t(), t(), acc -> acc)) :: acc when acc: term()
  def walk(ast, acc, fun), do: visit(ast, %__MODULE__{}, acc, fun)

  defp visit(node, env, acc, fun) do
    descend(node, env, fun.(node, env, acc), fun)
  end

  defp visit_all(nodes, env, acc, fun) do
    Enum.reduce(nodes, acc, &visit(&1, env, &2, fun))
  end

  defp descend({:__block__, _meta, statements}, env, acc, fun) when is_list(statements) do
    statements
    |> Enum.reduce({env, acc}, fn statement, {env, acc} ->
      {declare(statement, env), visit(statement, env, acc, fun)}
    end)
    |> elem(1)
  end

  defp descend(
         {{:., _, [{:__aliases__, _, segments}, @wrapper_fun]} = callee, _meta, args},
         env,
         acc,
         fun
       )
       when is_list(args) do
    if ModuleAliases.resolve(segments, env.aliases) == @wrapper_module,
      do: visit_all(args, %{env | replicated?: true}, acc, fun),
      else: visit_all([callee | args], env, acc, fun)
  end

  defp descend({@wrapper_fun, _meta, args}, %{wrapper_imported?: true} = env, acc, fun)
       when is_list(args),
       do: visit_all(args, %{env | replicated?: true}, acc, fun)

  defp descend({form, _meta, args}, env, acc, fun) when is_list(args),
    do: visit_all([form | args], env, acc, fun)

  defp descend({left, right}, env, acc, fun), do: visit_all([left, right], env, acc, fun)
  defp descend(nodes, env, acc, fun) when is_list(nodes), do: visit_all(nodes, env, acc, fun)
  defp descend(_leaf, _env, acc, _fun), do: acc

  defp declare({:import, _meta, [{:__aliases__, _, segments} | _opts]}, env) do
    if ModuleAliases.resolve(segments, env.aliases) == @wrapper_module,
      do: %{env | wrapper_imported?: true},
      else: env
  end

  defp declare(statement, env),
    do: %{env | aliases: ModuleAliases.declare(statement, env.aliases)}
end
