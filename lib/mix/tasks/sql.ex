defmodule Mix.Tasks.Sql do
  @moduledoc "Builds Logflare SQL"
  @shortdoc "Builds SQL"

  use Mix.Task

  @impl Mix.Task
  def run(_) do
    Mix.shell.cmd("./gradlew runtime", cd: "sql")
  end

end
