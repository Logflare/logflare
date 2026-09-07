defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Pool do
  @moduledoc false

  import Kernel, except: [send: 2]
  alias Logflare.Backends.Cache, as: BackendsCache
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Socket

  @behaviour NimblePool

  @connect_timeout to_timeout(second: 15)

  def start_link(opts) do
    backend_id = Keyword.fetch!(opts, :backend_id)
    name = Keyword.fetch!(opts, :name)
    worker_idle_timeout = Keyword.fetch!(opts, :worker_idle_timeout)

    NimblePool.start_link(
      worker: {__MODULE__, backend_id},
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: name
    )
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @spec send(NimblePool.pool(), iodata) :: :ok | {:error, reason}
        when reason: :closed | :timeout | :inet.posix() | :ssl.reason()
  def send(pool, message) do
    NimblePool.checkout!(pool, :checkout, fn from, conn ->
      with {:ok, socket} <- ensure_connected(conn, from),
           :ok <- Socket.send(socket, message) do
        {:ok, :keep}
      else
        {:error, reason} = error -> {error, {:remove, reason}}
      end
    end)
  end

  defp ensure_connected({:connected, socket}, _from), do: {:ok, socket}

  defp ensure_connected({:idle, backend_id}, from) do
    config = current_backend_config(backend_id)
    connect_and_transfer(config, from)
  end

  defp current_backend_config(backend_id) do
    if backend = BackendsCache.get_backend(backend_id) do
      backend.config
    else
      raise "missing backend #{backend_id}"
    end
  end

  defp connect_and_transfer(config, {owner, _ref} = from) do
    with {:ok, socket} <- Socket.connect(config, @connect_timeout) do
      :ok = NimblePool.update(from, {:connected, socket, config})

      case Socket.controlling_process(socket, owner) do
        :ok ->
          {:ok, socket}

        {:error, _reason} = error ->
          Socket.close(socket)
          error
      end
    end
  end

  @impl NimblePool
  def init_pool(backend_id) do
    {:ok, backend_id}
  end

  @impl NimblePool
  def init_worker(backend_id) do
    {:ok, :idle, backend_id}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, conn, backend_id) do
    case conn do
      :idle ->
        {:ok, {:idle, backend_id}, conn, backend_id}

      {:connected, socket, config} ->
        # if current backend config is the same as what it was when conn was opened,
        # return conn, otherwise remove it and try another one
        if config == current_backend_config(backend_id) do
          {:ok, {:connected, socket}, conn, backend_id}
        else
          {:remove, :stale_config, backend_id}
        end
    end
  end

  @impl NimblePool
  def handle_update({:connected, _socket, _config} = conn, :idle, backend_id) do
    {:ok, conn, backend_id}
  end

  @impl NimblePool
  def handle_checkin(:keep, _from, conn, backend_id) do
    {:ok, conn, backend_id}
  end

  def handle_checkin({:remove, reason}, _from, _conn, backend_id) do
    {:remove, reason, backend_id}
  end

  @impl NimblePool
  def handle_ping(_conn, _backend_id) do
    {:remove, :idle_timeout}
  end

  # NOTE: handle_info is called for every socket in the pool for every message received.
  # Since Syslog is typically a "write-only" protocol where we don't expect return traffic, this is probably fine.
  # The only expected messages are tcp_closed / tcp_error, which are rare.
  @impl NimblePool
  def handle_info(message, conn)

  def handle_info(message, {:connected, socket, _config} = conn) do
    case Socket.stream(socket, message) do
      {:error, reason} -> {:remove, reason}
      :ignore -> {:ok, conn}
    end
  end

  def handle_info(_message, :idle) do
    {:ok, :idle}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, backend_id) do
    with {:connected, socket, _config} <- conn, do: Socket.close(socket)
    {:ok, backend_id}
  end
end
