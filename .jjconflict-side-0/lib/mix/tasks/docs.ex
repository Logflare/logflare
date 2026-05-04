defmodule Logflare.MixTasks.Docs do
  @moduledoc "Copies Logflare docs to /priv, used for in-app documentation"
  @shortdoc "Copies docs to /priv"

  use Mix.Task

  @impl Mix.Task
  def run(_) do
    priv_docs_dir = Path.join(["priv", "docs"])
    Mix.shell().info("Cleaning #{priv_docs_dir}...")
    File.rm_rf!(priv_docs_dir)
    Mix.shell().info("Copying logflare docs...")
    File.cp_r!(Path.join(["docs", "docs.logflare.com", "docs"]), priv_docs_dir)
  end
end
