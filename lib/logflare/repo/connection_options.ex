defmodule Logflare.Repo.ConnectionOptions do
  @moduledoc """
  Resolves connection options shared by the primary repository and read replicas.
  """

  require Logger

  alias Logflare.Repo.AwsIam
  alias Logflare.Repo.Replicas
  alias Logflare.Utils

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
    config = resolve_url!(config)
    {auth, config} = Keyword.pop(config, :logflare_auth)
    {aws_region, config} = Keyword.pop(config, :logflare_aws_region)

    config
    |> prepare_auth(auth, aws_region)
    |> prepare_role(role)
  end

  @doc false
  @spec normalize_url_options(keyword()) :: {:ok, keyword()} | {:error, String.t()}
  def normalize_url_options(config) do
    {auth, config} = Keyword.pop(config, :auth)
    {aws_region, config} = Keyword.pop(config, :aws_region)

    with {:ok, config} <- put_auth(config, auth),
         {:ok, config} <- put_aws_region(config, aws_region) do
      {:ok, config}
    end
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

  @doc false
  @spec resolve_url!(keyword()) :: keyword()
  def resolve_url!(config) do
    case Keyword.pop(config, :url) do
      {url, config} when url in [nil, ""] ->
        config

      {url, config} when is_binary(url) ->
        uri = URI.parse(url)

        try do
          url_options = Ecto.Repo.Supervisor.parse_url(url)

          case normalize_url_options(url_options) do
            {:ok, url_options} ->
              merge_url_options(config, url_options)

            {:error, reason} ->
              raise ArgumentError,
                    "invalid database URL #{inspect(redact_url(url))}: #{reason}"
          end
        rescue
          error in Ecto.InvalidURLError ->
            redacted_url = redact_url(url)

            message =
              error.message
              |> String.replace(url, redacted_url)
              |> redact_userinfo(uri.userinfo)

            reraise %{error | message: message, url: redacted_url}, __STACKTRACE__
        end

      {url, _config} ->
        raise ArgumentError, "database URL must be a string, got: #{inspect(url)}"
    end
  end

  defp merge_url_options(config, url_options) do
    socket_options =
      config
      |> Keyword.get(:socket_options)
      |> then(&(&1 || []))
      |> Enum.reject(&(&1 in [:inet, :inet6]))

    url_options =
      config
      |> preserve_configured_ssl(url_options)
      |> maybe_put_socket_options(socket_options)

    config
    |> Keyword.delete(:socket_options)
    |> Keyword.merge(url_options)
  end

  defp preserve_configured_ssl(config, url_options) do
    if is_list(config[:ssl]) and url_options[:ssl] == true do
      Logger.warning(
        "ignoring `ssl=true` parameter in URL because `ssl` is already set in the configuration: #{inspect(config[:ssl])}"
      )

      Keyword.delete(url_options, :ssl)
    else
      url_options
    end
  end

  defp maybe_put_socket_options(config, socket_options) do
    case Utils.ip_version(config[:hostname]) do
      version when version in [:inet, :inet6] ->
        Keyword.put(config, :socket_options, [version | socket_options])

      _other when socket_options == [] ->
        config

      _other ->
        Keyword.put(config, :socket_options, socket_options)
    end
  end

  defp redact_url(url) do
    uri = URI.parse(url)
    URI.to_string(%{uri | userinfo: if(uri.userinfo, do: "REDACTED")})
  end

  defp redact_userinfo(message, nil), do: message
  defp redact_userinfo(message, userinfo), do: String.replace(message, userinfo, "REDACTED")

  defp put_auth(config, nil) do
    if Keyword.has_key?(config, :password),
      do: {:ok, Keyword.put(config, :logflare_auth, :password)},
      else: {:ok, config}
  end

  defp put_auth(config, "password"),
    do: {:ok, Keyword.put(config, :logflare_auth, :password)}

  defp put_auth(config, "aws_iam") do
    if Keyword.has_key?(config, :password),
      do: {:error, "auth=aws_iam cannot be combined with a password"},
      else: {:ok, Keyword.put(config, :logflare_auth, :aws_iam)}
  end

  defp put_auth(_config, other),
    do: {:error, ~s(unsupported auth=#{other}, expected "password" or "aws_iam")}

  defp put_aws_region(config, nil), do: {:ok, config}

  defp put_aws_region(config, region) when is_binary(region) and region != "",
    do: {:ok, Keyword.put(config, :logflare_aws_region, region)}

  defp put_aws_region(_config, _region), do: {:error, "aws_region cannot be empty"}

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
