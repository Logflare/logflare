defmodule Logflare.Repo.ConnectionOptions do
  @moduledoc """
  Resolves connection options shared by the primary repository and read replicas.
  """

  alias Logflare.Repo.AwsIam
  alias Logflare.Repo.Replicas

  @epgsql_connection_keys [:hostname, :port, :username, :database, :socket_options, :ssl]
  @ecto_repo_only_options [
    :log,
    :migration_repo,
    :name,
    :otp_app,
    :pool,
    :pool_count,
    :pool_size,
    :queue_interval,
    :queue_target,
    :start_apps_before_migration,
    :telemetry_prefix
  ]

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
  Prepares a single direct Postgrex connection from the primary repository config.

  Ecto repository and pool options are removed so clients such as the cluster
  strategy cannot inherit the primary repository's pool size or registration.
  """
  @spec prepare_postgrex(keyword()) :: keyword()
  def prepare_postgrex(config) do
    config
    |> prepare(:primary)
    |> Keyword.drop(@ecto_repo_only_options)
  end

  @doc """
  Prepares primary connection options for epgsql-based clients.

  Connection callbacks resolve epgsql's fixed connection options once, then run
  again during authentication to refresh the password. A callback cannot change
  those fixed options after initialization.
  """
  @spec prepare_epgsql(keyword()) :: map()
  def prepare_epgsql(config) do
    {config, password} = config |> prepare(:primary) |> prepare_epgsql_config()
    hostname = Keyword.fetch!(config, :hostname)

    %{}
    |> put_present(:host, String.to_charlist(hostname))
    |> put_present(:port, config[:port])
    |> put_present(:username, config[:username])
    |> put_present(:database, config[:database])
    |> put_present(:password, password)
    |> put_present(:tcp_opts, config[:socket_options])
    |> put_epgsql_ssl(config[:ssl], hostname)
  end

  defp prepare_epgsql_config(config) do
    case config[:configure] do
      nil ->
        {config, config[:password]}

      {AwsIam, :configure, [region, previous_configure]} ->
        prepared = AwsIam.configure_options(config, previous_configure)

        password = fn ->
          configured = AwsIam.configure(config, region, previous_configure)
          ensure_epgsql_connection_unchanged!(prepared, configured)
          Keyword.fetch!(configured, :password)
        end

        {prepared, password}

      configure ->
        prepared = run_configure(config, configure)

        password = fn ->
          configured = run_configure(config, configure)
          ensure_epgsql_connection_unchanged!(prepared, configured)
          Keyword.fetch!(configured, :password)
        end

        {prepared, password}
    end
  end

  defp ensure_epgsql_connection_unchanged!(prepared, configured) do
    case Enum.find(@epgsql_connection_keys, &(prepared[&1] != configured[&1])) do
      nil ->
        :ok

      key ->
        raise ArgumentError,
              "epgsql configure callback changed #{inspect(key)} after connection initialization"
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
