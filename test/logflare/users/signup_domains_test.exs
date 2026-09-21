defmodule Logflare.Users.SignupDomainsTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Logflare.Users.SignupDomains

  doctest Logflare.Users.SignupDomains

  describe "parse!/1" do
    test "returns an empty list for nil and for an empty string" do
      assert SignupDomains.parse!(nil) == []
      assert SignupDomains.parse!("") == []
      assert SignupDomains.parse!(" , ,") == []
    end

    test "normalizes case, spaces, a leading @, and duplicates" do
      assert SignupDomains.parse!(" Supabase.com,@supabase.io,supabase.com ") ==
               ["supabase.com", "supabase.io"]
    end

    test "accepts a subdomain, a hyphen, and a single label" do
      assert SignupDomains.parse!("mail.supabase.com,my-company.io,localhost") ==
               ["mail.supabase.com", "my-company.io", "localhost"]
    end

    test "raises for an entry that is not a domain name" do
      invalid_values = [
        "supabase.com;supabase.io",
        "*.supabase.com",
        "https://supabase.com",
        "supabase.com supabase.io",
        "supabase..com",
        "-supabase.com",
        "dev@supabase.com"
      ]

      for value <- invalid_values do
        assert_raise ArgumentError, ~r/LOGFLARE_SIGNUP_ALLOWED_DOMAINS/, fn ->
          SignupDomains.parse!(value)
        end
      end
    end
  end

  describe "rejection_message/0" do
    test "returns the message for a rejected signup" do
      assert SignupDomains.rejection_message() =~ "restricted to approved email domains"
    end
  end

  describe "allowed_domains/0" do
    test "defaults to an empty list" do
      assert SignupDomains.allowed_domains() == []
    end
  end

  describe "allowed?/1 without a configured list" do
    test "allows every email address" do
      assert SignupDomains.allowed?("someone@example.com")
      assert SignupDomains.allowed?("no-at-sign")
      assert SignupDomains.allowed?(nil)
    end
  end

  describe "allowed?/1 with a configured list" do
    setup do
      stub(SignupDomains, :allowed_domains, fn -> ["supabase.com", "supabase.io"] end)
      :ok
    end

    test "allows an email address on a listed domain" do
      assert SignupDomains.allowed?("dev@supabase.com")
      assert SignupDomains.allowed?("dev@supabase.io")
      assert SignupDomains.allowed?("  Dev@SUPABASE.com ")
    end

    test "rejects an email address on another domain" do
      refute SignupDomains.allowed?("dev@example.com")
      refute SignupDomains.allowed?("dev@supabase.com.example.com")
      refute SignupDomains.allowed?("dev@notsupabase.com")
    end

    test "rejects a subdomain of a listed domain" do
      refute SignupDomains.allowed?("dev@mail.supabase.com")
    end

    test "uses the text after the last @" do
      refute SignupDomains.allowed?("dev@supabase.com@example.com")
      assert SignupDomains.allowed?("\"odd@local\"@supabase.com")
    end

    test "rejects an email address with an empty local part" do
      refute SignupDomains.allowed?("@supabase.com")
    end

    test "rejects a value without a domain" do
      refute SignupDomains.allowed?("supabase.com")
      refute SignupDomains.allowed?("dev@")
      refute SignupDomains.allowed?("")
      refute SignupDomains.allowed?(nil)
    end
  end
end
