defmodule Logflare.Redix do
  @default_conn :logflare_redix

  def child_spec(_args) do
    host = Application.get_env(:logflare, @default_conn)[:host]
    port = Application.get_env(:logflare, @default_conn)[:port]
    # Specs for the Redix connections.
    children = [
      Supervisor.child_spec({Redix, name: @default_conn, host: host, port: port},
        id: {Redix, 0}
      )
    ]

    # Spec for the supervisor that will supervise the Redix connections.
    %{
      id: RedixSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def increment(key) do
    command(["INCR", key])
  end

  def increment(key, opts) do
    expire = opts[:expire]

    if opts && expire do
      pipeline([["INCR", key], ["EXPIRE", key, expire]])
    else
      increment(key)
    end
  end

  def pipeline(commands) when is_list(commands) do
    Redix.pipeline(@default_conn, commands)
  end

  def get(key) do
    command(["GET", key])
  end

  def set(key, value) do
    command(["SET", key, value])
  end

  def set(key, value, opts) do
    expire = opts[:expire]

    if opts && expire do
      Redix.command(@default_conn, ["SET", key, value, "EX", expire])
    else
      set(key, value)
    end
  end

  def command(args) when is_list(args) do
    Redix.command(@default_conn, args)
  end

  def scan(opts) do
    command(["SCAN", opts[:cursor], "MATCH", opts[:match], "COUNT", opts[:count]])
  end

  def scan_all_match(match) do
    with {:ok, ["0", keys]} <- command(["SCAN", 0, "MATCH", match, "COUNT", 100_000]) do
      {:ok, keys}
    else
      errtup -> errtup
    end
  end

  def multi_get([]) do
    {:error, :empty_keys_list}
  end

  def multi_get(keys) when is_list(keys) do
    command(["MGET" | keys])
  end
end
