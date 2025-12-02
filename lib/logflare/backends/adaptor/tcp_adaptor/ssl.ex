defmodule Logflare.Backends.Adaptor.TCPAdaptor.SSL do
  @moduledoc false

  def opts(address, opts \\ []) do
    hostname(address, opts)
    |> default_opts()
    |> Keyword.merge(opts)
    |> add_verify_opts()
    |> remove_incompatible_opts()
  end

  defp hostname(address, opts) when is_list(opts) do
    case Keyword.fetch(opts, :hostname) do
      {:ok, hostname} ->
        hostname

      :error when is_binary(address) ->
        address

      :error ->
        raise ArgumentError, "the :hostname option is required when address is not a binary"
    end
  end

  defp default_opts(hostname) do
    [
      server_name_indication: String.to_charlist(hostname),
      verify: :verify_peer,
      depth: 100,
      secure_renegotiate: true,
      reuse_sessions: true
    ]
  end

  defp add_verify_opts(opts) do
    verify = Keyword.get(opts, :verify)

    if verify == :verify_peer do
      opts
      |> add_cacerts()
      |> add_customize_hostname_check()
    else
      opts
    end
  end

  defp remove_incompatible_opts(opts) do
    # These are the TLS versions that are compatible with :reuse_sessions and :secure_renegotiate
    # If none of the compatible TLS versions are present in the transport options, then
    # :reuse_sessions and :secure_renegotiate will be removed from the transport options.
    compatible_versions = [:tlsv1, :"tlsv1.1", :"tlsv1.2"]
    versions_opt = Keyword.get(opts, :versions, [])

    if Enum.any?(compatible_versions, &(&1 in versions_opt)) do
      opts
    else
      opts
      |> Keyword.delete(:reuse_sessions)
      |> Keyword.delete(:secure_renegotiate)
    end
  end

  defp add_customize_hostname_check(opts) do
    match_fun = :public_key.pkix_verify_hostname_match_fun(:https)
    Keyword.put_new(opts, :customize_hostname_check, match_fun: match_fun)
  end

  defp add_cacerts(opts) do
    if Keyword.has_key?(opts, :cacerts) do
      opts
    else
      Keyword.put(opts, :cacerts, :public_key.cacerts_get())
    end
  end
end
