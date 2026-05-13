defmodule Logflare.Vault do
  @doc """
  GenServer needed for Cloak.
  It handles secrets migration for key rolling at startup.

  To run the migration at runtime, use the following:
  ```elixir
  iex> Logflare.Vault.do_migrate()
  ```
  An old encryption key should be present for the migration.

  """
  use Cloak.Vault, otp_app: :logflare

  alias Cloak.Ecto.Migrator
  require Logger

  @schemas [
    Logflare.Backends.Backend
  ]

  @impl GenServer
  def init(config) do
    if Application.get_env(:logflare, :env) == :test do
      # make ets table public
      :ets.new(@table_name, [:named_table, :public])
    end

    fallback_key = Application.get_env(:logflare, :encryption_key_fallback) |> maybe_decode!()

    default_key =
      Application.get_env(:logflare, :encryption_key_default) |> maybe_decode!() || fallback_key

    retired_key = Application.get_env(:logflare, :encryption_key_retired) |> maybe_decode!()

    ciphers =
      [
        default:
          {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1." <> hash(default_key), key: default_key},
        retired:
          if(retired_key != nil,
            do: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1" <> hash(retired_key), key: retired_key},
            else: nil
          ),
        fallback:
          {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1." <> hash(fallback_key), key: fallback_key}
      ]
      |> Enum.filter(fn {_k, v} -> v != nil end)

    config = Keyword.put(config, :ciphers, ciphers)

    Task.start_link(fn ->
      # wait for genserver config to be saved, see https://github.com/danielberkompas/cloak/blob/v1.1.4/lib/cloak/vault.ex#L186
      :timer.sleep(1_000)

      result =
        if retired_key != nil do
          Logger.info("Encryption key marked as 'retired' found, migrating schemas to new key.")

          do_migrate()
        else
          :noop
        end

      if result != :noop do
        Logger.info("Encryption migration complete")
      end
    end)

    {:ok, config}
  end

  # helper, exposed for testing
  def do_migrate do
    for schema <- @schemas do
      Migrator.migrate(Logflare.Repo, schema)
    end
  end

  # helper for loading keys
  defp maybe_decode!(nil), do: nil
  defp maybe_decode!(str), do: Base.decode64!(str)

  # used to hash the tag based on the key, as cloak uses the tag to determine cipher to use.
  defp hash(key) do
    :sha256 |> :crypto.hash(key) |> Base.encode64()
  end

  # helper for tests
  def get_config do
    Cloak.Vault.read_config(@table_name)
  end

  # helper for tests
  def save_config(config) do
    Cloak.Vault.save_config(@table_name, config)
  end

  # helper for tests
  def get_cipher(key) do
    key = key |> maybe_decode!()
    {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1" <> hash(key), key: key}
  end
end
