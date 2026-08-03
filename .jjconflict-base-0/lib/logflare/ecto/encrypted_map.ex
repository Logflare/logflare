defmodule Logflare.Ecto.EncryptedMap do
  use Cloak.Ecto.Map, vault: Logflare.Vault

  @type t :: map()
end
