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

  @doc """
  Prepares primary connection options for epgsql-based clients.

  epgsql invokes a zero-arity password function while authenticating, so dynamic
  Postgrex configuration callbacks remain lazy and are rerun after reconnects.
  """
  @spec prepare_epgsql(keyword()) :: map()
  def prepare_epgsql(config) do
    config = prepare(config, :primary)
    hostname = Keyword.fetch!(config, :hostname)

    %{}
    |> put_present(:host, String.to_charlist(hostname))
    |> put_present(:port, config[:port])
    |> put_present(:username, config[:username])
    |> put_present(:database, config[:database])
    |> put_present(:password, epgsql_password(config))
    |> put_present(:tcp_opts, config[:socket_options])
    |> put_epgsql_ssl(config[:ssl], hostname)
  end

  defp epgsql_password(config) do
    case config[:configure] do
      nil ->
        config[:password]

      configure ->
        fn ->
          config
          |> run_configure(configure)
          |> Keyword.fetch!(:password)
        end
    end
  end

  defp run_configure(opts, {module, function, args}),
    do: apply(module, function, [opts | args])

  defp run_configure(opts, configure) when is_function(configure, 1), do: configure.(opts)

  defp put_epgsql_ssl(options, ssl, _hostname) when ssl in [nil, false], do: options

  defp put_epgsql_ssl(options, ssl, hostname) when ssl == true or is_list(ssl) do
    ssl_options =
      if(is_list(ssl), do: ssl, else: [])
      |> Keyword.put_new(:verify, :verify_peer)
      |> maybe_put_system_cacerts()
      |> Keyword.put_new(
        :customize_hostname_check,
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      )
      |> Keyword.put_new(:server_name_indication, epgsql_sni(hostname))

    options
    |> Map.put(:ssl, :required)
    |> Map.put(:ssl_opts, ssl_options)
  end

  defp maybe_put_system_cacerts(options) do
    if options[:verify] == :verify_peer and
         not (Keyword.has_key?(options, :cacerts) or Keyword.has_key?(options, :cacertfile)) do
      Keyword.put(options, :cacerts, :public_key.cacerts_get())
    else
      options
    end
  end

  defp epgsql_sni(hostname) do
    case :inet.parse_address(String.to_charlist(hostname)) do
      {:ok, _address} -> :disable
      {:error, _reason} -> String.to_charlist(hostname)
    end
  end

  defp put_present(options, _key, nil), do: options
  defp put_present(options, key, value), do: Map.put(options, key, value)

  defp prepare_auth(config, :aws_iam, aws_region) do
    config
    |> Keyword.update(:start_apps_before_migration, [:ex_aws], fn apps ->
      Enum.uniq([:ex_aws | apps])
    end)
    |> AwsIam.put_connection_options(aws_region)
  end

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
