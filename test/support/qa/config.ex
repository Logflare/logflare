defmodule Logflare.QA.Config do
  @moduledoc """
  Settings shared by `mix qa.server`, `mix qa.ingest` and `mix qa.ui`. Each reads an
  environment variable with a default, so the server and the tasks agree without flags.
  """

  @spec node_name() :: atom()
  def node_name, do: :logflare_qa

  @spec server_node() :: node()
  def server_node do
    {:ok, host} = :inet.gethostname()
    :"#{node_name()}@#{host}"
  end

  @spec cookie() :: atom()
  def cookie, do: :logflare_qa

  @spec url() :: String.t()
  def url, do: System.get_env("LOGFLARE_URL", "http://localhost:4000")

  @spec grpc_port() :: String.t()
  def grpc_port, do: "50051"

  @doc "Single-tenant public access token. Use a value this dev database has not used before."
  @spec public_token() :: String.t()
  def public_token, do: System.get_env("QA_PUBLIC_TOKEN", "logflare-qa-public-token")

  @spec backend_url() :: String.t()
  def backend_url,
    do:
      System.get_env(
        "QA_BACKEND_URL",
        "postgresql://postgres:postgres@localhost:5432/logflare_qa_backend"
      )

  @spec output_dir() :: Path.t()
  def output_dir, do: Path.join(File.cwd!(), "tmp/qa")
end
