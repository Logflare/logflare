defmodule Logflare.CredoChecks.ObanMigrationReplication do
  use Credo.Check,
    category: :warning,
    base_priority: :high,
    explanations: [
      check: """
      `Oban.Migration.up/1` and `Oban.Migration.down/1` emit a mix of Ecto DSL DDL and
      raw SQL strings (`CREATE TYPE ... oban_job_state`, the notify function, triggers).

      Under `Logflare.Repo.Pglogical` the DSL DDL is replicated but the raw SQL is not,
      so a subscriber receives `CREATE TABLE oban_jobs` for an enum type it never got and
      the apply worker stalls.

      Wrap the call so every statement it flushes is replicated:

          # preferred

          def up do
            Migrator.with_replicated_execute(fn -> Oban.Migration.up(version: 12) end)
          end

          # NOT preferred

          def up, do: Oban.Migration.up(version: 12)
      """
    ]

  alias Logflare.CredoChecks.ReplicatedExecuteScope

  @oban_migration_funs [:up, :down]

  @impl true
  def run(%SourceFile{} = source_file, params) do
    issue_meta = IssueMeta.for(source_file, params)
    ast = Credo.Code.ast(source_file)
    scopes = ReplicatedExecuteScope.line_ranges(ast)

    ast
    |> Macro.prewalk([], &traverse(&1, &2, issue_meta, scopes))
    |> elem(1)
    |> Enum.reverse()
  end

  defp traverse(
         {{:., _, [{:__aliases__, _, aliases}, fun]}, meta, _args} = ast,
         issues,
         issue_meta,
         scopes
       )
       when fun in @oban_migration_funs do
    if oban_migration_alias?(aliases) and not ReplicatedExecuteScope.within?(scopes, meta[:line]) do
      {ast, [issue_for(issue_meta, meta, aliases, fun) | issues]}
    else
      {ast, issues}
    end
  end

  defp traverse(ast, issues, _issue_meta, _scopes), do: {ast, issues}

  defp oban_migration_alias?(aliases) do
    case Enum.take(aliases, -2) do
      [:Oban, :Migration] -> true
      [:Oban, :Migrations] -> true
      _ -> false
    end
  end

  defp issue_for(issue_meta, meta, aliases, fun) do
    trigger = Enum.map_join(aliases, ".", &Atom.to_string/1) <> ".#{fun}"

    format_issue(
      issue_meta,
      message:
        "Wrap #{trigger} in Logflare.Repo.Migrator.with_replicated_execute/1 so the raw SQL it emits is replicated to pglogical subscribers.",
      line_no: meta[:line],
      column: meta[:column]
    )
  end
end
