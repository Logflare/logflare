defmodule Logflare.Utils.SSRF.TCP do
  @moduledoc """
  TCP callback module that prevents connections to non-public IP addresses.

  It is intended for OTP's classic `:inet` backend via the `:tcp_module`
  transport option. DNS answers from both address families are filtered before
  OTP attempts them, and every numeric address is checked again immediately
  before the socket is opened.
  """

  alias Logflare.Utils.SSRF

  @blocked_reason :eacces

  @type address :: String.t() | :inet.hostname() | :inet.ip_address()
  @type timer :: false | reference()

  @doc "Raises unless OTP is using the classic `:inet` socket backend."
  @spec ensure_supported_backend!() :: :ok
  def ensure_supported_backend! do
    case :inet.inet_backend() do
      :inet ->
        :ok

      backend ->
        raise "#{inspect(__MODULE__)} requires the :inet backend, got: #{inspect(backend)}"
    end
  end

  @spec getaddr(address()) :: {:ok, :inet.ip_address()} | {:error, atom()}
  def getaddr(address), do: address |> getaddrs() |> first_address()

  @spec getaddr(address(), timer()) :: {:ok, :inet.ip_address()} | {:error, atom()}
  def getaddr(address, timer), do: address |> getaddrs(timer) |> first_address()

  @spec getaddrs(address()) :: {:ok, [:inet.ip_address(), ...]} | {:error, atom()}
  def getaddrs(address) when is_binary(address), do: getaddrs(String.to_charlist(address))

  def getaddrs(address) do
    resolve_all(address, &:inet_tcp.getaddrs/1, &:inet6_tcp.getaddrs/1)
  end

  @spec getaddrs(address(), timer()) ::
          {:ok, [:inet.ip_address(), ...]} | {:error, atom()}
  def getaddrs(address, timer) when is_binary(address),
    do: getaddrs(String.to_charlist(address), timer)

  def getaddrs(address, timer) do
    resolve_all(
      address,
      &:inet_tcp.getaddrs(&1, timer),
      &:inet6_tcp.getaddrs(&1, timer)
    )
  end

  @spec getserv(:inet.port_number() | atom()) :: {:ok, :inet.port_number()} | {:error, atom()}
  def getserv(port), do: :inet_tcp.getserv(port)

  @spec connect(:inet.ip_address(), :inet.port_number(), list()) ::
          {:ok, :inet.socket()} | {:error, atom()}
  def connect(address, port, opts) when is_integer(port) do
    connect(address, port, opts, :infinity)
  end

  @spec connect(map(), list(), timeout()) :: {:ok, :inet.socket()} | {:error, atom()}
  def connect(%{addr: address} = socket_address, opts, timeout) do
    with :ok <- validate_address(address) do
      tcp_module(address).connect(socket_address, opts, timeout)
    end
  end

  def connect(_socket_address, _opts, _timeout), do: {:error, @blocked_reason}

  @spec connect(:inet.ip_address(), :inet.port_number(), list(), timeout()) ::
          {:ok, :inet.socket()} | {:error, atom()}
  def connect(address, port, opts, timeout) do
    with :ok <- validate_address(address) do
      tcp_module(address).connect(address, port, opts, timeout)
    end
  end

  defp resolve_all(address, resolve_ipv4, resolve_ipv6) do
    ipv4_result = resolve_ipv4.(address)
    ipv6_result = resolve_ipv6.(address)

    case {ipv4_result, ipv6_result} do
      {{:error, reason}, {:error, _reason}} ->
        {:error, reason}

      _results ->
        ipv4_addresses = ipv4_result |> result_addresses() |> SSRF.public_addresses()
        ipv6_addresses = ipv6_result |> result_addresses() |> SSRF.public_addresses()

        case interleave(ipv4_addresses, ipv6_addresses) do
          [] -> {:error, @blocked_reason}
          [_ | _] = addresses -> {:ok, addresses}
        end
    end
  end

  defp result_addresses({:ok, addresses}), do: addresses
  defp result_addresses({:error, _reason}), do: []

  defp first_address({:ok, [address | _]}), do: {:ok, address}
  defp first_address({:error, _reason} = error), do: error

  defp interleave([], right), do: right
  defp interleave(left, []), do: left

  defp interleave([left | left_rest], [right | right_rest]) do
    [left, right | interleave(left_rest, right_rest)]
  end

  defp validate_address(address) do
    if :inet.is_ip_address(address) and not SSRF.private_ip?(address) do
      :ok
    else
      {:error, @blocked_reason}
    end
  end

  defp tcp_module(address) when tuple_size(address) == 4, do: :inet_tcp
  defp tcp_module(address) when tuple_size(address) == 8, do: :inet6_tcp
end
