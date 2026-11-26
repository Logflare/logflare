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

  describe "offset_page/2" do
    test "starts at offset 0 without a page_token" do
      assert {:ok, {10, 0}} = PageToken.offset_page(1, page_size: 10)
    end

    test "resumes from the offset in the page_token" do
      token = PageToken.encode(1, %{"offset" => 20})
      assert {:ok, {10, 20}} = PageToken.offset_page(1, page_size: 10, page_token: token)
    end

    test "is nil without a page_size" do
      assert {:ok, nil} = PageToken.offset_page(1, [])
    end

    test "rejects a page_token for another user" do
      token = PageToken.encode(2, %{"offset" => 20})

      assert {:error, :invalid_page_token} =
               PageToken.offset_page(1, page_size: 10, page_token: token)
    end
  end

  describe "split_offset_page/3" do
    test "returns a next page token when there is an extra row" do
      assert {[1, 2], token} = PageToken.split_offset_page([1, 2, 3], {2, 4}, 1)
      assert {:ok, %{"offset" => 6}} = PageToken.decode(1, token)
    end

    test "returns no token on the last page" do
      assert {[1, 2], nil} = PageToken.split_offset_page([1, 2], {2, 4}, 1)
    end

    test "passes rows through when not paging" do
      assert {[1, 2, 3], nil} = PageToken.split_offset_page([1, 2, 3], nil, 1)
    end
  end
end
