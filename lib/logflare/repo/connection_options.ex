defmodule Logflare.Repo.ConnectionOptions do
  @moduledoc """
  Resolves connection options shared by the primary repository and read replicas.
  """

  alias Logflare.Repo.AwsIam
  alias Logflare.Repo.Replicas

  @type role :: :primary | :replica

  @spec prepare(keyword(), role()) :: keyword()
  def prepare(config, role) when role in [:primary, :replica] do
    {auth, config} = Keyword.pop(config, :logflare_auth)
    {aws_region, config} = Keyword.pop(config, :logflare_aws_region)

    config
    |> prepare_auth(auth, aws_region)
    |> prepare_role(role)
  end

  defp prepare_auth(config, :aws_iam, aws_region),
    do: AwsIam.put_connection_options(config, aws_region)

  defp prepare_auth(config, auth, _aws_region) when auth in [nil, :password], do: config

  defp prepare_auth(_config, auth, _aws_region),
    do: raise(ArgumentError, "unsupported database authentication mode: #{inspect(auth)}")

  defp prepare_role(config, :primary), do: config

  defp prepare_role(config, :replica) do
    primary_after_connect = config[:after_connect]

    Keyword.put(
      config,
      :after_connect,
      {Replicas, :after_connect, [primary_after_connect]}
    )
  end
end
