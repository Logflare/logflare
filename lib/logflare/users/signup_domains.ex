defmodule Logflare.Users.SignupDomains do
  @moduledoc """
  Restricts new accounts to a configured list of email domains.

  The list comes from the `LOGFLARE_SIGNUP_ALLOWED_DOMAINS` environment variable, for
  example `supabase.com,supabase.io`. An empty list allows every domain.

  Only a new user or a new team member is checked. An existing account can sign in from
  any domain. The match is exact and case-insensitive: `mail.supabase.com` does not
  match `supabase.com`.
  """

  @doc """
  Parses a comma-separated list of domains.

  ## Examples

      iex> Logflare.Users.SignupDomains.parse("supabase.com, Supabase.IO ,@example.com,,")
      ["supabase.com", "supabase.io", "example.com"]

      iex> Logflare.Users.SignupDomains.parse(nil)
      []
  """
  @spec parse(String.t() | nil) :: [String.t()]
  def parse(nil), do: []

  def parse(value) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&normalize_domain/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  @doc """
  Domains that can create a new account. An empty list means no restriction.
  """
  @spec allowed_domains() :: [String.t()]
  def allowed_domains, do: Application.get_env(:logflare, :signup_allowed_domains, [])

  @doc """
  Returns true if an account with this email address can be created.
  """
  @spec allowed?(String.t() | nil) :: boolean()
  def allowed?(email), do: allowed?(email, __MODULE__.allowed_domains())

  @spec allowed?(String.t() | nil, [String.t()]) :: boolean()
  defp allowed?(_email, []), do: true

  defp allowed?(email, domains) when is_binary(email) do
    case email |> String.trim() |> String.downcase() |> String.split("@") do
      [_local | _] = parts when length(parts) > 1 -> List.last(parts) in domains
      _no_domain -> false
    end
  end

  defp allowed?(_email, _domains), do: false

  @spec normalize_domain(String.t()) :: String.t()
  defp normalize_domain(domain) do
    domain
    |> String.trim()
    |> String.downcase()
    |> String.trim_leading("@")
  end
end
