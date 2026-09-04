defmodule Logflare.Utils.SSRF.TCP do
  @moduledoc """
  TCP callback module that prevents connections to non-public IP addresses.

  It is intended for OTP's classic `:inet` backend via the `:tcp_module`
  transport option. Address families are resolved concurrently, every collected
  DNS answer is filtered before OTP attempts it, and every numeric address is
  checked again immediately before the socket is opened.
  """

  alias Logflare.Utils.SSRF
  alias Logflare.Utils.SSRF.TCP.Resolver

  @blocked_reason :eacces
  @resolution_grace_ms 25

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

  def getaddrs(address), do: resolve_all(address, false)

  @spec getaddrs(address(), timer()) ::
          {:ok, [:inet.ip_address(), ...]} | {:error, atom()}
  def getaddrs(address, timer) when is_binary(address),
    do: getaddrs(String.to_charlist(address), timer)

  def getaddrs(address, timer), do: resolve_all(address, timer)

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

  defp resolve_all(address, timer) do
    started_at = System.monotonic_time(:millisecond)

    tasks =
      Enum.map([:inet, :inet6], fn family ->
        Task.async(fn -> {family, Resolver.getaddrs(address, family, timer)} end)
      end)

    try do
      await_resolutions(tasks, %{}, started_at, timer)
    after
      Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
    end
  end

  defp await_resolutions([], results, _started_at, _timer), do: resolution_result(results)

  defp await_resolutions(tasks, results, started_at, timer) do
    case resolution_wait(results, started_at, timer) do
      :ready ->
        finalize_resolution(tasks, results)

      :timeout ->
        {:error, :timeout}

      timeout ->
        case yield_resolution(tasks, timeout) do
          {:ok, task, family, result} ->
            await_resolutions(
              List.delete(tasks, task),
              Map.put(results, family, filter_result(result)),
              started_at,
              timer
            )

          :timeout ->
            resolution_timeout_result(tasks, results)
        end
    end
  end

  defp resolution_timeout_result(tasks, results) do
    if usable_result?(results), do: finalize_resolution(tasks, results), else: {:error, :timeout}
  end

  defp finalize_resolution(tasks, results) do
    results =
      tasks
      |> Task.yield_many(0)
      |> Enum.reduce(results, fn
        {_task, {:ok, {family, result}}}, results ->
          Map.put(results, family, filter_result(result))

        {_task, {:exit, reason}}, _results ->
          exit(reason)

        {_task, nil}, results ->
          results
      end)

    resolution_result(results)
  end

  defp resolution_wait(results, started_at, timer) do
    remaining_timeout = :inet.timeout(timer)

    cond do
      usable_result?(results) -> resolution_grace_wait(started_at, remaining_timeout)
      remaining_timeout == 0 -> :timeout
      true -> remaining_timeout
    end
  end

  defp resolution_grace_wait(started_at, remaining_timeout) do
    elapsed = System.monotonic_time(:millisecond) - started_at
    grace_remaining = max(@resolution_grace_ms - elapsed, 0)

    grace_remaining
    |> cap_resolution_wait(remaining_timeout)
    |> case do
      0 -> :ready
      wait -> wait
    end
  end

  defp cap_resolution_wait(grace_remaining, :infinity), do: grace_remaining

  defp cap_resolution_wait(grace_remaining, remaining_timeout) do
    min(grace_remaining, max(remaining_timeout - @resolution_grace_ms, 0))
  end

  defp yield_resolution(tasks, timeout) do
    tasks
    |> Task.yield_many(limit: 1, timeout: timeout)
    |> Enum.find_value(:timeout, fn
      {task, {:ok, {family, result}}} -> {:ok, task, family, result}
      {_task, {:exit, reason}} -> exit(reason)
      {_task, nil} -> false
    end)
  end

  defp filter_result({:ok, addresses}), do: {:ok, SSRF.public_addresses(addresses)}
  defp filter_result({:error, _reason} = error), do: error

  defp usable_result?(results) do
    Enum.any?(results, fn
      {_family, {:ok, [_address | _addresses]}} -> true
      {_family, _result} -> false
    end)
  end

  defp resolution_result(results) do
    ipv4_result = Map.get(results, :inet)
    ipv6_result = Map.get(results, :inet6)

    case {ipv4_result, ipv6_result} do
      {{:error, reason}, {:error, _reason}} ->
        {:error, reason}

      _results ->
        ipv4_addresses = result_addresses(ipv4_result)
        ipv6_addresses = result_addresses(ipv6_result)

        case interleave(ipv4_addresses, ipv6_addresses) do
          [] -> {:error, @blocked_reason}
          [_ | _] = addresses -> {:ok, addresses}
        end
    end
  end

  defp result_addresses({:ok, addresses}), do: addresses
  defp result_addresses(nil), do: []
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
