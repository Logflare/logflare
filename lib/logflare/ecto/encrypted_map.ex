defmodule Logflare.Ecto.EncryptedMap do
  use Cloak.Ecto.Map, vault: Logflare.Vault
end
