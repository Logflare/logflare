defmodule Logflare.Vault do
  @doc """
  GenServer needed for Cloak.
  It handles secrets migration for key rolling at startup.

  To run the migration at runtime, use the following:
  ```elixir
  iex> Logflare.Vault.do_migration()
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
    default_key = Application.get_env(:logflare, :encryption_key_default) |> maybe_decode!()
    old_key = Application.get_env(:logflare, :encryption_key_old) |> maybe_decode!()

    ciphers =
      [
        default: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1", key: default_key},
        old:
          if(is_nil(old_key),
            do: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1", key: old_key},
            else: nil
          )
      ]
      |> Enum.filter(fn {_k, v} -> v != nil end)

    config = Keyword.put(config, :ciphers, ciphers)

    {:ok, config, {:continue, :migrate}}
  end

  @impl GenServer
  def handle_continue(:migrate, config) do
    ciphers = Keyword.get(config, :ciphers)

    if Keyword.has_key?(ciphers, :old) do
      Logger.info("Encryption key marked as 'old' found, migrating schemas to new key.")
      do_migrate()
    end

    {:noreply, config}
  end

  def do_migrate() do
    for schema <- @schemas do
      Migrator.migrate(Logflare.Repo, schema)
    end
  end

  defp maybe_decode!(nil), do: nil
  defp maybe_decode!(str), do: Base.decode64!(str)
end
