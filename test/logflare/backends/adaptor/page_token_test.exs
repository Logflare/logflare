defmodule Logflare.Backends.Adaptor.PageTokenTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.PageToken

  test "decodes a token for the user it was encoded for" do
    token = PageToken.encode(1, %{"offset" => 100})

    assert is_binary(token)
    assert {:ok, %{"offset" => 100}} = PageToken.decode(1, token)
  end

  test "rejects a token encoded for another user" do
    token = PageToken.encode(1, %{"offset" => 100})

    assert {:error, :invalid_page_token} = PageToken.decode(2, token)
  end

  test "rejects a tampered token" do
    assert {:error, :invalid_page_token} = PageToken.decode(1, "not-a-token")
  end
end
