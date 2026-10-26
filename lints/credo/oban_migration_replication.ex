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

  alias Logflare.CredoChecks.ModuleAliases
  alias Logflare.CredoChecks.ReplicatedExecuteScope

  @oban_migration_funs [:up, :down]
  @oban_migration_modules [[:Oban, :Migration], [:Oban, :Migrations]]

  @impl true
  def run(%SourceFile{} = source_file, params) do
    issue_meta = IssueMeta.for(source_file, params)

    source_file
    |> SourceFile.ast()
    |> ReplicatedExecuteScope.walk([], &collect_issue(&1, &2, &3, issue_meta))
    |> Enum.reverse()
  end

  defp collect_issue(
         {{:., _, [{:__aliases__, _, segments}, fun]}, meta, _args},
         %ReplicatedExecuteScope{replicated?: false} = env,
         issues,
         issue_meta
       )
       when fun in @oban_migration_funs do
    module = ModuleAliases.resolve(segments, env.aliases)

    if module in @oban_migration_modules,
      do: [issue_for(issue_meta, meta, module, fun) | issues],
      else: issues
  end

  defp collect_issue(_node, _env, issues, _issue_meta), do: issues

  defp issue_for(issue_meta, meta, module, fun) do
    trigger = Enum.map_join(module, ".", &Atom.to_string/1) <> ".#{fun}"

    format_issue(
      issue_meta,
      message:
        "Wrap #{trigger} in Logflare.Repo.Migrator.with_replicated_execute/1 so the raw SQL it emits is replicated to pglogical subscribers.",
      line_no: meta[:line],
      column: meta[:column]
    )
  end
end
