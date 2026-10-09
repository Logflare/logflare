defmodule Logflare.QA.Config do
  @moduledoc """
  Settings shared by `mix qa.server`, `mix qa.ingest` and `mix qa.ui`.

  Each setting reads an environment variable and falls back to a default, so the
  server and the QA tasks agree without extra flags.
  """

  @spec node_name() :: atom()
  def node_name, do: String.to_atom(env("QA_NODE", "logflare_qa"))

  @spec server_node() :: node()
  def server_node do
    {:ok, host} = :inet.gethostname()
    :"#{node_name()}@#{host}"
  end

  @spec cookie() :: atom()
  def cookie, do: String.to_atom(env("QA_COOKIE", "logflare_qa"))

  @spec port() :: String.t()
  def port, do: env("QA_PORT", "4000")

  @spec grpc_port() :: String.t()
  def grpc_port, do: env("QA_GRPC_PORT", "50051")

  @spec url() :: String.t()
  def url, do: env("LOGFLARE_URL", "http://localhost:#{port()}")

  @doc "Single-tenant public access token. Use a value this dev database has not used before."
  @spec public_token() :: String.t()
  def public_token, do: env("QA_PUBLIC_TOKEN", "logflare-qa-public-token")

  @spec backend_url() :: String.t()
  def backend_url,
    do:
      env(
        "QA_BACKEND_URL",
        "postgresql://postgres:postgres@localhost:5432/logflare_qa_backend"
      )

  @doc "Where captures and their expectation manifests are written."
  @spec output_dir() :: Path.t()
  def output_dir, do: env("QA_OUTPUT_DIR", Path.join(File.cwd!(), "tmp/qa"))

  defp env(name, default), do: System.get_env(name) || default
end
