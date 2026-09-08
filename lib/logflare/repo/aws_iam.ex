defmodule Logflare.Repo.AwsIam do
  @moduledoc """
  Configures PostgreSQL connections to authenticate with AWS RDS IAM tokens.
  """

  require Logger

  @iam_token_expiry_seconds 900
  @default_rds_ca_cert_path "/etc/ssl/certs/aws-rds-global-bundle.pem"

  @type callback ::
          {module(), atom(), [term()]}
          | (keyword() -> keyword())
          | nil

  @spec put_connection_options(keyword(), String.t() | nil) :: keyword()
  def put_connection_options(config, region) do
    region = validate_region!(region)
    validate_hostname!(config[:hostname])
    {configure, config} = Keyword.pop(config, :configure)

    config
    |> put_verified_ssl!()
    |> Keyword.put(:configure, {__MODULE__, :configure, [region, configure]})
  end

  @doc """
  Replaces the password with a freshly minted RDS IAM authentication token.

  DBConnection invokes this callback before every connection attempt.
  """
  @spec configure(keyword(), String.t(), callback()) :: keyword()
  def configure(opts, region, previous_configure) do
    opts = run_configure(opts, previous_configure)
    hostname = Keyword.fetch!(opts, :hostname)
    username = Keyword.fetch!(opts, :username)
    port = Keyword.get(opts, :port) || 5432

    Keyword.put(opts, :password, auth_token(hostname, port, username, region))
  end

  @doc """
  Builds an RDS IAM authentication token used in place of a password.
  """
  @spec auth_token(String.t(), non_neg_integer(), String.t(), String.t()) :: String.t()
  def auth_token(hostname, port, username, region) do
    hostname = hostname |> validate_hostname!() |> String.downcase()
    region = validate_region!(region)

    config = ExAws.Config.new(:rds, aws_config_options(region))

    {:ok, url} =
      ExAws.Auth.presigned_url(
        :get,
        "https://#{hostname}:#{port}/",
        :"rds-db",
        NaiveDateTime.to_erl(NaiveDateTime.utc_now()),
        config,
        @iam_token_expiry_seconds,
        [{"Action", "connect"}, {"DBUser", username}]
      )

    String.replace_prefix(url, "https://", "")
  end

  defp put_verified_ssl!(config) do
    ssl = config[:ssl]

    cond do
      ssl == false ->
        raise ArgumentError, "AWS IAM authentication requires TLS; remove ssl=false"

      ssl in [nil, true] ->
        Keyword.put(config, :ssl, cacerts: trusted_cacerts())

      is_list(ssl) ->
        validate_ssl_options!(ssl)

        ssl =
          ssl
          |> Keyword.drop([:server_name_indication, :customize_hostname_check, :verify_fun])
          |> maybe_put_cacerts()

        Keyword.put(config, :ssl, ssl)

      true ->
        raise ArgumentError,
              "AWS IAM authentication requires ssl=true or verified SSL options"
    end
  end

  defp validate_ssl_options!(opts) do
    if Keyword.get(opts, :verify, :verify_peer) != :verify_peer do
      raise ArgumentError, "AWS IAM authentication requires verify: :verify_peer"
    end
  end

  defp maybe_put_cacerts(opts) do
    if Keyword.has_key?(opts, :cacerts) or Keyword.has_key?(opts, :cacertfile) do
      opts
    else
      Keyword.put(opts, :cacerts, trusted_cacerts())
    end
  end

  defp trusted_cacerts do
    system_cacerts = :public_key.cacerts_get()
    path = Application.get_env(:logflare, :rds_ca_cert_path, @default_rds_ca_cert_path)

    case File.read(path) do
      {:ok, pem} ->
        case decode_cacerts(pem) do
          [] ->
            Logger.warning(
              "AWS RDS CA bundle #{inspect(path)} contains no certificates; using system CA certificates"
            )

            system_cacerts

          rds_cacerts ->
            system_cacerts ++ rds_cacerts
        end

      {:error, reason} ->
        Logger.warning(
          "AWS RDS CA bundle #{inspect(path)} is unavailable: #{:file.format_error(reason)}; " <>
            "using system CA certificates"
        )

        system_cacerts
    end
  end

  defp decode_cacerts(pem) do
    pem
    |> :public_key.pem_decode()
    |> Enum.flat_map(fn
      {:Certificate, der, _} -> [{:cert, der, :public_key.pkix_decode_cert(der, :otp)}]
      _ -> []
    end)
  end

  defp aws_config_options(region) do
    opts = [region: region]

    case Application.get_env(:ex_aws, :security_token) do
      token when is_binary(token) and token != "" ->
        Keyword.put(opts, :security_token, token)

      _ ->
        if static_env_credentials?(), do: put_static_security_token(opts), else: opts
    end
  end

  defp put_static_security_token(opts) do
    case System.get_env("AWS_SESSION_TOKEN") do
      token when is_binary(token) and token != "" -> Keyword.put(opts, :security_token, token)
      _ -> Keyword.put(opts, :security_token, nil)
    end
  end

  defp static_env_credentials? do
    is_nil(Application.get_env(:ex_aws, :access_key_id)) and
      is_nil(Application.get_env(:ex_aws, :secret_access_key)) and
      Enum.all?(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"], fn key ->
        case System.get_env(key) do
          value when is_binary(value) and value != "" -> true
          _ -> false
        end
      end)
  end

  defp run_configure(opts, nil), do: opts

  defp run_configure(opts, {module, function, args}),
    do: apply(module, function, [opts | args])

  defp run_configure(opts, fun) when is_function(fun, 1), do: fun.(opts)

  defp validate_hostname!(hostname) when is_binary(hostname) and hostname != "" do
    case :inet.parse_address(String.to_charlist(hostname)) do
      {:ok, _address} -> raise ArgumentError, "AWS IAM authentication requires a DNS hostname"
      {:error, _reason} -> hostname
    end
  end

  defp validate_hostname!(_hostname),
    do: raise(ArgumentError, "AWS IAM authentication requires a DNS hostname")

  defp validate_region!(region) when is_binary(region) do
    case region |> String.trim() |> String.downcase() do
      "" -> raise ArgumentError, "AWS IAM authentication requires an AWS region"
      region -> region
    end
  end

  defp validate_region!(_region),
    do: raise(ArgumentError, "AWS IAM authentication requires an AWS region")
end
