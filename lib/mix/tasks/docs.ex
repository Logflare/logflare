defmodule Mix.Tasks.Docs do
  @moduledoc "Copies Logflare docs to /priv, used for in-app documentation"
  @shortdoc "Copies docs to /priv"

  use Mix.Task

  @impl Mix.Task
  def run(_) do
    # TODO: convert commands to platform agonostic
    IO.puts("Cleaning /priv/docs...")
    Mix.shell().cmd("rm -rf docs", cd: "priv")
    IO.puts("Copying logflare docs...")
    Mix.shell().cmd("cp -r docs ../../priv", cd: "docs/docs.logflare.com")
  end
end
