defmodule Logflare.QA.Remote do
  @moduledoc """
  Runs QA code inside the node started by `mix qa.server`.

  `connect!/0` loads the current object code of every `Logflare.QA` module onto the
  server node, so a changed QA module needs no server restart. Remote functions
  return data and the caller prints it: output from the server node does not reach
  the caller's terminal.
  """

  alias Logflare.QA.Config

  @spec connect!() :: node()
  def connect! do
    server = Config.server_node()

    if node() == :nonode@nohost do
      {_, 0} = System.cmd("epmd", ["-daemon"])

      {:ok, _} =
        Node.start(:"qa_client_#{System.unique_integer([:positive])}", name_domain: :shortnames)
    end

    Node.set_cookie(Config.cookie())
    Node.connect(server) || Mix.raise("Cannot connect to #{server}. Is `mix qa.server` running?")

    _ = Application.load(:logflare)
    {:ok, modules} = :application.get_key(:logflare, :modules)

    for module <- modules, String.starts_with?(Atom.to_string(module), "Elixir.Logflare.QA.") do
      {^module, binary, file} = :code.get_object_code(module)
      {:module, ^module} = :erpc.call(server, :code, :load_binary, [module, file, binary])
    end

    server
  end

  @spec call(module(), atom(), list()) :: term()
  def call(module, fun, args), do: :erpc.call(Config.server_node(), module, fun, args, :infinity)
end
